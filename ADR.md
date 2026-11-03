# ADR-004: Inventory Decrement Processing Architecture

## Status

Accepted

## Context

Hệ thống thương mại điện tử hiện tại gặp sự cố nghiêm trọng (downtime, HTTP 504 Gateway Timeout) trong các khung giờ cao điểm và sự kiện Flash Sale. Qua công tác giám sát hạ tầng và điều tra nguyên nhân gốc rễ (Root Cause Analysis - RCA), các vấn đề kỹ thuật được xác định như sau:

1. **Cơ chế trừ kho trực tiếp vào RDBMS (Synchronous Direct Decrement):**
   - Khi phát sinh đơn hàng, hệ thống thực hiện trừ kho trực tiếp thông qua câu lệnh SQL cập nhật trạng thái kho trong MySQL (sử dụng Storage Engine InnoDB) với mệnh đề điều kiện dạng:
     ```sql
     UPDATE inventory 
     SET available_stock = available_stock - ? 
     WHERE sku_id = ? AND available_stock >= ?;
     ```
   - Trong các đợt Flash Sale, lưu lượng truy cập dồn tập trung vào một số lượng nhỏ các mặt hàng bán chạy (Hotspot SKUs). Hàng ngàn luồng (threads) đồng thời cố gắng cập nhật trên cùng một bản ghi (row).

2. **Cạnh tranh khóa mức hàng (Row-level Lock Contention) và suy giảm hiệu năng:**
   - InnoDB sử dụng cơ chế khóa độc quyền (Exclusive Lock - X-lock) trên index record khi thực thi câu lệnh `UPDATE`.
   - Khi hàng ngàn transaction đồng thời tranh chấp một row lock, các transaction phải xếp hàng chờ trong hàng đợi khóa (`lock wait`).
   - Thời gian chờ khóa gia tăng kéo theo thời gian giữ kết nối (Connection Hold Time) kéo dài. Kết quả là toàn bộ các connection trong pool kết nối của ứng dụng (ví dụ: HikariCP) nhanh chóng bị chiếm dụng và rơi vào trạng thái cạn kiệt (Connection Pool Exhaustion).

3. **Lỗi dây chuyền (Cascading Failure):**
   - Khi pool kết nối cơ sở dữ liệu cạn kiệt, các luồng xử lý yêu cầu (Application Server Worker Threads) bị phong tỏa do không lấy được kết nối database.
   - Hàng đợi yêu cầu (Request Queue) bị tràn, làm cạn kiệt tài nguyên CPU và bộ nhớ của Web/Application Server.
   - Cơ sở dữ liệu MySQL bị quá tải do số lượng transaction mở và luồng chờ khóa tăng cao, dẫn đến nguy cơ Deadlock, kéo sập các dịch vụ dùng chung cơ sở dữ liệu và làm tê liệt toàn bộ hệ thống.

Để đáp ứng lưu lượng tải tăng đột biến, loại bỏ điểm nghẽn cổ chai tại database và đảm bảo tính khả dụng của hệ thống, chúng ta cần tái kiến trúc cơ chế trừ kho theo hướng tách rời (decoupling), tối ưu hóa tốc độ ghi và bảo vệ cơ sở dữ liệu quan hệ.

## Decision

Chúng tôi quyết định thay thế cơ chế trừ kho đồng bộ vào MySQL bằng mô hình kiến trúc **Kết hợp Đệm bộ nhớ nguyên tử với Xử lý bất đồng bộ (Hybrid Redis Atomic Pre-decrement & Kafka Asynchronous Persistence)**.

### Chi tiết kiến trúc giải pháp

Kiến trúc mới phân tách luồng xử lý thành hai pha độc lập: Pha kiểm soát trực tiếp tại bộ nhớ (In-Memory Fast-Path) và Pha đồng bộ lưu trữ bền vững (Reliable Async Persistence Path).

```
[Client / API Gateway]
          │
          ▼
[Order / Inventory Service]
          │
          ├── (1) Atomic Decrement (Lua Script)
          ▼
   [Redis Cluster]
   (Kiểm tra stock & trừ kho tức thời)
          │
     Thành công? ──► [Không đủ hàng] ──► Trả về Out-of-Stock ngay lập tức
          │
     [Có đủ hàng]
          │
          ├── (2) Publish InventoryDecrementedEvent
          ▼
   [Apache Kafka Topic]
   (inventory-decrements, Partitioned by SKU)
          │
          ├── (3) Controlled Batch Consumption
          ▼
[Inventory Sync Worker]
          │
          ├── (4) Batch UPDATE / Idempotent Write
          ▼
   [MySQL Database]
   (Lưu trữ trạng thái bền vững)
```

1. **Pha In-Memory Fast-Path (Redis Atomic Operations):**
   - Thông tin tồn kho khả dụng (`available_stock`) của các mặt hàng được đồng bộ và lưu trữ trên cụm Redis.
   - Thao tác kiểm tra tồn kho và trừ kho được đóng gói trong một **Redis Lua Script** để đảm bảo tính nguyên tử (Atomicity), loại bỏ Race Condition mà không cần cơ chế phân tán khóa phức tạp (Distributed Lock Overhead):
     - Lua script kiểm tra điều kiện `available_stock >= requested_quantity`.
     - Nếu thỏa mãn, script thực hiện giảm số lượng tồn kho qua lệnh `DECRBY` và trả về kết quả thành công.
     - Nếu không đủ tồn kho, script dừng ngay lập tức và trả về mã lỗi hết hàng.
   - Nhờ bản chất đơn luồng (Single-threaded event loop) trong quá trình thực thi Lua script của Redis, toàn bộ thao tác diễn ra ở mức microsecond, không phát sinh cạnh tranh khóa tại cơ sở dữ liệu và cho phép trả phản hồi gần như tức thời về cho phía người dùng (Fast-fail đối với các yêu cầu không hợp lệ hoặc hết hàng).

2. **Pha Reliable Async Persistence Path (Apache Kafka):**
   - Sau khi trừ kho thành công trên Redis, service phát hành một sự kiện `InventoryDecrementedEvent` vào hàng đợi thông điệp **Apache Kafka** (topic `inventory-decrements`).
   - Khóa phân vùng (Partition Key) của message được cấu hình theo `sku_id` nhằm đảm bảo tính tuần tự tuyệt đối của các giao dịch trên cùng một SKU trong phạm vi partition.
   - Nhóm Consumer (`Inventory Sync Worker`) đăng ký nhận sự kiện từ Kafka và thực hiện đồng bộ vào cơ sở dữ liệu MySQL theo lô (Batch Processing) hoặc điều tiết tốc độ (Rate Limiting).
   - Cơ sở dữ liệu MySQL được bảo vệ hoàn toàn khỏi lưu lượng tăng đột biến, chuyển từ việc phải xử lý hàng chục ngàn câu lệnh ghi đồng thời sang cơ chế xử lý tuần tự, mượt mà và kiểm soát được tải (Traffic Leveling / Buffering).

3. **Cơ chế Đảm bảo Nhất quán và Xử lý Lỗi (Consistency & Fault Tolerance):**
   - **Xử lý đơn hàng thất bại/hủy:** Trường hợp thanh toán thất bại hoặc người dùng hủy đơn, sự kiện hoàn kho (`InventoryCompensatedEvent`) được gửi qua Kafka để hoàn lại số lượng tồn kho trên Redis (sử dụng `INCRBY`) và đồng bộ lại MySQL.
   - **Đảm bảo tính Idempotency:** Mỗi sự kiện trừ kho gắn liền với một `transaction_id` hoặc `order_id` duy nhất. Phía Consumer kiểm tra tính duy nhất (Idempotent Consumer Pattern) trước khi ghi vào MySQL để tránh trừ kho trùng lặp khi Kafka rebalance hoặc retry.
   - **Đối soát định kỳ (Reconciliation Job):** Một background job chạy định kỳ để đối chiếu dữ liệu tồn kho giữa Redis và MySQL, phát hiện và hiệu chỉnh độ lệch dữ liệu phát sinh do các lỗi biên mạng hoặc lỗi hệ thống ngoài dự kiến.

## Consequences

Quyết định kiến trúc này mang lại các thay đổi quan trọng đối với đặc tính vận hành và kiến trúc của toàn hệ thống:

### Ưu điểm (Positive)

- **Loại bỏ hoàn toàn Database Lock Contention:** Áp lực ghi đồng thời trực tiếp lên một row trong bảng MySQL được chuyển đổi thành các phép tính toán nguyên tử tại Redis và các thao tác ghi có điều tiết qua Kafka. Hiện tượng lock wait và connection pool exhaustion tại cơ sở dữ liệu được triệt tiêu.
- **Tăng cường năng lực chịu tải và độ phản hồi (High Throughput & Low Latency):** Thao tác trừ kho phản hồi khách hàng được xử lý trực tiếp trên RAM của Redis với thời gian trễ cực thấp. Khách hàng nhận được kết quả giữ chỗ tồn kho gần như tức thời.
- **Bảo vệ hệ thống hạ tầng (Backpressure & Traffic Leveling):** Kafka đóng vai trò như một hồ đệm hấp thụ toàn bộ xung nhịp lưu lượng (burst traffic). Tốc độ ghi xuống MySQL hoàn toàn do các worker chủ động điều tiết theo năng lực tối đa của cơ sở dữ liệu.
- **Khả năng mở rộng độc lập (Horizontal Scalability):** Tầng xử lý nghiệp vụ, tầng đệm (Redis Cluster), tầng truyền thông điệp (Kafka Cluster) và tầng ghi bền vững (MySQL Worker) có thể scale độc lập theo nhu cầu thực tế.

### Nhược điểm (Negative)

- **Gia tăng độ phức tạp kiến trúc và hạ tầng:** Hệ thống phụ thuộc thêm vào hai cụm phân tán trọng yếu là Redis Cluster và Apache Kafka Cluster. Điều này đòi hỏi chi phí đầu tư hạ tầng, nhân lực vận hành và công cụ giám sát chuyên sâu (theo dõi consumer lag, memory usage, replication delay).
- **Rủi ro thất thoát dữ liệu trên In-Memory:** Bộ nhớ Redis là volatile. Dù có các cơ chế persistence như RDB snapshot và AOF (Append-Only File), việc cấu hình `appendfsync everysec` vẫn tồn tại cửa sổ rủi ro mất mát dữ liệu nhỏ nếu nút Redis master gặp sự cố phần cứng đột ngột trước khi kịp ghi đĩa hoặc replicate sang replica.
- **Xử lý lỗi phân tán phức tạp:** Cần hiện thực hóa các giải pháp kiểm soát lỗi nâng cao như Dead-Letter Queue (DLQ), retry logic có exponential backoff và cơ chế bù trừ giao dịch (Compensating Transactions) khi có bước thực thi thất bại giữa chừng.

### Đánh đổi (Trade-offs)

- **Tính nhất quán tức thời (Strong Consistency) đổi lấy Tính nhất quán cuối cùng (Eventual Consistency):**
  - Hệ thống từ bỏ mô hình ACID phân tán chặt chẽ giữa trạng thái bộ nhớ và đĩa cứng để đổi lấy tính sẵn sàng cao (High Availability) và hiệu năng vượt trội theo định lý CAP.
  - Tồn tại một khoảng trễ thời gian (lag) nhất định giữa số lượng tồn kho phản ánh trên Redis và số liệu ghi nhận trong bảng MySQL. Ứng dụng chấp nhận việc số liệu tồn kho trong MySQL là "eventually consistent" miễn là dữ liệu hiển thị trên Redis luôn chính xác trong suốt quá trình bán hàng.
- **Chi phí phát triển và vận hành (Engineering Effort) đổi lấy Khả năng chống chịu sự cố (System Resilience):**
  - Đội ngũ kỹ thuật phải đầu tư nhiều công sức hơn để xây dựng các kịch bản đối soát dữ liệu (reconciliation), xử lý idempotency và khôi phục sự cố sau thảm họa, nhằm đánh đổi lại một hệ thống vững chắc không còn nguy cơ sập vào các dịp cao điểm.
