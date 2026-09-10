# XCare Omnichannel Platform - Enterprise Pharmacy & Retail Backend

Hệ thống Quản lý Chuỗi Bán lẻ Dược & Thương mại Điện tử Đa kênh (Omnichannel Pharmacy Retail & Ecommerce) theo kiến trúc Microservices chuẩn Enterprise.

---

## 📌 Task 1: Kịch bản Hủy đơn thuốc & Kích hoạt Saga Rollback (Compensating Transactions Flow)

### 1. Bối cảnh nghiệp vụ (Business Scenario)
- **Tình huống**: Khách hàng đặt mua đơn thuốc trên App XCare. Đơn hàng đã qua khâu duyệt đơn và gán chuyến giao dịch; hiện tại **Shipper (đối tác 3PL như Ahamove / GHTK) đang trên đường di chuyển tới Nhà thuốc chi nhánh để nhận hàng**.
- **Hành động của khách**: Khách hàng ấn nút **Hủy đơn** trên App.
- **Thách thức kiến trúc**: Hệ thống bao gồm nhiều Microservices với cơ chế **Database-per-Service**. Không thể dùng Distributed Transaction (2PC) vì làm giảm thông lượng và tăng coupling. Do đó, hệ thống phải kích hoạt chuỗi **Saga Rollback (Compensating Actions)** bất đồng bộ thông qua **Apache Kafka 3.7.0**, đảm bảo tính nhất quán cuối cùng (**Eventual Consistency**).

---

### 2. Các Kỹ năng & Kiến thức cốt lõi (Skills Learned)

| # | Kỹ năng / Pattern | Công nghệ áp dụng | Chi tiết thực hiện trong Task 1 |
|---|-------------------|-------------------|---------------------------------|
| **1** | **Saga Pattern (Choreography Rollback)** | Apache Kafka 3.7.0, Spring Cloud | Điều phối chuỗi hoàn tác qua 3 services: Order Service -> Shipping Service -> Inventory Service -> Order Service mà không phụ thuộc vào Orchestrator tập trung. |
| **2** | **Transactional Outbox Pattern** | PostgreSQL 16, Spring Data JPA, Scheduling | Ở Bước 1, cập nhật trạng thái đơn sang `CANCEL_REQUESTED` và ghi sự kiện vào bảng `outbox_events` trong **cùng một ACID Transaction** cục bộ. Worker nền sử dụng `SELECT ... FOR UPDATE SKIP LOCKED` để gửi Kafka đảm bảo At-Least-Once Delivery không rớt dữ liệu. |
| **3** | **Idempotent Consumer Pattern** | Redis 7.2 (Spring Data Redis) | Tránh xử lý trùng lặp khi Kafka rebalance hoặc retry mạng. Áp dụng Redis Key `xcare:idempotency:shipping:cancel:{orderId}` và `xcare:idempotency:inventory:release:{orderId}` sử dụng `setIfAbsent` kèm TTL 24 giờ. |
| **4** | **Distributed MultiLock chống Deadlock** | Redis 7.2 + Redisson 3.42.0 | Khi hoàn trả tồn kho nhiều SKU ở Bước 3, danh sách SKU được **sắp xếp theo thứ tự từ điển (Lexicographical Sort)** trước khi gọi `redissonClient.getMultiLock()`, triệt tiêu 100% rủi ro Circular Distributed Deadlock khi nhiều đơn hoàn trả/giữ kho đồng thời. |
| **5** | **Database Migration per Service** | Liquibase 4.29.2 | Quản lý schema độc lập cho từng microservice: `orders_db`, `shipping_db`, `inventory_db`, tuân thủ nguyên tắc `spring.jpa.hibernate.ddl-auto=none`. |
| **6** | **Exception Handling & Defensive Programming** | Spring Boot `@RestControllerAdvice` | Xử lý chặt chẽ `IllegalOrderStateException`, validation ràng buộc trạng thái đơn hàng, timeout an toàn khi gọi 3PL Partner REST API. |

---

### 3. Quy trình 4 Bước Saga Rollback Chi tiết

```text
[Khách hàng App XCare]
       │
       │ 1. POST /api/v1/orders/{orderId}/cancel
       ▼
┌────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 1: Order Service (:8081)                                          │
│ - Kiểm tra điều kiện đơn hàng (cho phép hủy khi shipper đang lấy hàng)  │
│ - Chuyển trạng thái Order -> CANCEL_REQUESTED                          │
│ - Ghi sự kiện vào bảng outbox_events (Status: PENDING)                 │
│ - DB Transaction Commit!                                               │
│ - Outbox Publisher quét & bắn Kafka: 'order-cancellation-events'       │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   │ Kafka Event: OrderCancelRequestedEvent
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 2: Shipping Service (:8082)                                       │
│ - Kiểm tra Redis Idempotency: xcare:idempotency:shipping:cancel:{id}   │
│ - Gọi Mock 3PL REST API (Ahamove/GHTK Cancel Delivery API)             │
│ - Cập nhật trạng thái chuyến xe: ShipmentStatus -> CANCELLED           │
│ - Đánh dấu Redis Idempotency = COMPLETED (TTL 24h)                     │
│ - Bắn Kafka Event: 'shipping-cancellation-events'                      │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   │ Kafka Event: ShipmentCancelledEvent
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 3: Hub Fulfillment & Inventory Service (:8083)                    │
│ - Kiểm tra Redis Idempotency: xcare:idempotency:inventory:release:{id} │
│ - Sort danh sách SKU theo Alphabet để tránh Distributed Deadlock       │
│ - Acquire Redisson MultiLock: xcare:lock:inventory:{hubId}:{sku}       │
│ - Hoàn tác kho: reserved_quantity -= qty, available_quantity += qty    │
│ - Release Redisson MultiLock an toàn trong khối finally                │
│ - Đánh dấu Redis Idempotency = COMPLETED (TTL 24h)                     │
│ - Bắn Kafka Event: 'inventory-events' (Status: INVENTORY_RELEASED)     │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   │ Kafka Event: InventoryReleasedEvent
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 4: Order Service (:8081)                                          │
│ - Kafka Listener nhận 'inventory-events'                               │
│ - Kiểm tra điều kiện đơn đang ở CANCEL_REQUESTED                       │
│ - Cập nhật Order Status -> CANCELLED_BY_CUSTOMER                       │
│ - Ghi log kiểm toán đơn thuốc (Prescription Audit Log)                 │
│ - Hoàn tất chu kỳ Saga Rollback thành công!                            │
└────────────────────────────────────────────────────────────────────────┘
```

---

### 4. Danh sách các File và Module đã triển khai trong Task 1

#### 📦 `order-service`
- `domain/enums/OrderStatus.java`: Bổ sung `CANCEL_REQUESTED`, `CANCELLED_BY_CUSTOMER`.
- `dto/request/CancelOrderRequest.java`: Payload nhận lý do hủy đơn từ khách hàng.
- `dto/response/CancelOrderResponse.java`: Trả về kết quả tiếp nhận yêu cầu hủy và Saga Correlation ID.
- `event/OrderCancelRequestedEvent.java`: DTO sự kiện gửi sang Kafka topic `order-cancellation-events`.
- `event/InventoryReleasedEvent.java`: DTO nhận sự kiện từ Inventory Service.
- `exception/IllegalOrderStateException.java`: Xử lý ngoại lệ khi đơn không ở trạng thái hợp lệ để hủy.
- `service/OrderService.java` & `impl/OrderServiceImpl.java`: Logic kiểm tra, đổi trạng thái và ghi Outbox table.
- `controller/OrderController.java`: Endpoint `POST /api/v1/orders/{orderId}/cancel`.
- `listener/OrderSagaKafkaListener.java`: Lắng nghe hoàn tất hoàn trả kho và chốt trạng thái đơn.

#### 🚚 `shipping-service`
- `domain/Shipment.java` & `domain/ShipmentStatus.java`: Entity quản lý vận chuyển.
- `client/ThirdPartyLogisticsClient.java`: Adapter gọi API hủy chuyến Ahamove / GHTK với cơ chế retry và timeout.
- `event/OrderCancelRequestedEvent.java` & `event/ShipmentCancelledEvent.java`.
- `service/ShippingCancellationService.java`: Idempotency Redis + Hủy chuyến 3PL + Lưu DB + Bắn Kafka.
- `listener/ShippingCancellationKafkaListener.java`: Lắng nghe topic `order-cancellation-events`.

#### 🏢 `fulfillment-service` (Inventory Management)
- `inventory/domain/HubStock.java`: Entity quản lý tồn kho khả dụng (`availableQuantity`) và tạm giữ (`reservedQuantity`).
- `inventory/repository/HubStockRepository.java`: Truy vấn kho chi nhánh theo hubId & sku.
- `inventory/service/InventoryReleaseService.java`: Sắp xếp SKU, Redisson MultiLock, hoàn trả tồn kho, giải phóng lock, ghi nhận Idempotency và bắn `INVENTORY_RELEASED`.
- `inventory/listener/InventorySagaKafkaListener.java`: Lắng nghe topic `shipping-cancellation-events`.
- `inventory/controller/InventoryController.java`: API truy vấn tồn kho và health check.

#### 🗄️ Database & Migration Scripts
- `init-scripts/01-init-databases.sql`: Tạo cơ sở dữ liệu `orders_db`, `shipping_db`, `fulfillment_db`, `inventory_db`, `auth_db`, `keycloak_db`.
- `shipping-service/src/main/resources/db/changelog/`: Liquibase migration tạo bảng `shipments`.
- `fulfillment-service/src/main/resources/db/changelog/`: Liquibase migration tạo bảng `hub_stocks`.

---

### 5. Hướng dẫn Chạy & Kiểm thử (Verification)

#### Khởi động hạ tầng Docker:
```bash
docker compose up -d
```
Hạ tầng gồm:
- PostgreSQL 16 (Port 5432)
- Apache Kafka 3.7.0 KRaft (Port 9092, 29092)
- Kafka UI (Port 8090 - http://localhost:8090)
- Redis 7.2 (Port 6379)
- Keycloak 24.0.2 (Port 8088)

#### Kích hoạt kịch bản Hủy đơn qua REST API:
```bash
curl -X POST http://localhost:8081/api/v1/orders/{orderId}/cancel \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "reason": "Khách hàng đổi ý muốn đổi sang thuốc viên sủi"
  }'
```

#### Quan sát luồng thực thi trong Log:
1. **Order Service**: `[BƯỚC 1] Tiếp nhận yêu cầu hủy đơn ... Đổi trạng thái sang CANCEL_REQUESTED và lưu outbox event`.
2. **Shipping Service**: `[BƯỚC 2] Nhận yêu cầu hủy chuyến ... Gọi API 3PL đối tác thành công ... Bắn SHIPMENT_CANCELLED sang Kafka`.
3. **Inventory Service**: `[BƯỚC 3] Nhận SHIPMENT_CANCELLED ... Lấy Redisson MultiLock cho các SKU ... Hoàn trả số lượng tồn kho thành công`.
4. **Order Service**: `[BƯỚC 4] Nhận INVENTORY_RELEASED ... Hoàn tất quy trình Saga! Cập nhật trạng thái đơn thành CANCELLED_BY_CUSTOMER`.
