# XCare Omnichannel Platform - Enterprise Pharmacy & Retail Backend

Hệ thống Quản lý Chuỗi Bán lẻ Dược & Thương mại Điện tử Đa kênh (Omnichannel Pharmacy Retail & Ecommerce) theo kiến trúc Microservices chuẩn Enterprise.

---

## 📌 Task 1: Kịch bản Hủy đơn thuốc & Kích hoạt Saga Rollback (Saga Orchestration 5 Bước)

### Tóm tắt Siêu Ngắn Gọn (Architecture Summary - Command-Response Star Topology)

Mô hình **Saga Orchestration** do `Order Service (:8081)` làm Central Orchestrator điều phối tập trung thông qua các Kafka Topic chuyên biệt theo dạng Command-Response:

```
                  ┌────────────────────────────────────────────────────────┐
                  │          ORDER SERVICE (:8081) - ORCHESTRATOR           │
                  └───────────────┬────────────────────────▲───────────────┘
  1. CancelShipmentCommand        │                        │  2. ShipmentCancelledEvent
  [Topic: 'shipping-commands']    │                        │  [Topic: 'order-saga-responses']
                                  ▼                        │
                  ┌────────────────────────────────────────┴───────────────┐
                  │                 SHIPPING SERVICE (:8082)               │
                  │ - Redis 7.2 Idempotency (TTL 24h)                      │
                  │ - Gọi API 3PL (Ahamove/GHTK) hủy chuyến xe             │
                  │ - Cập nhật Shipment CANCELLED & lưu Outbox             │
                  └────────────────────────────────────────────────────────┘

                  ┌────────────────────────────────────────────────────────┐
                  │          ORDER SERVICE (:8081) - ORCHESTRATOR           │
                  └───────────────┬────────────────────────▲───────────────┘
  3. RevertInventoryCommand       │                        │  4. InventoryReleasedEvent
  [Topic: 'inventory-commands']   │                        │  [Topic: 'order-saga-responses']
                                  ▼                        │
                  ┌────────────────────────────────────────┴───────────────┐
                  │           FULFILLMENT / INVENTORY SERVICE (:8083)      │
                  │ - Redis 7.2 Idempotency (TTL 24h)                      │
                  │ - Sắp xếp SKU Alphabet (chống Distributed Deadlock)    │
                  │ - Giữ Redisson MultiLock 3.42.0 đa SKU                 │
                  │ - Hoàn tác kho: reserved -= qty, available += qty      │
                  │ - Giải phóng Lock an toàn & lưu Outbox                 │
                  └────────────────────────────────────────────────────────┘
```

#### Quy trình 5 Bước chuẩn:
1. **Bước 1 (Order Service - Orchestrator)**:
   - `POST /api/v1/orders/{orderId}/cancel` $\rightarrow$ Validate đơn (không cho hủy khi đã `DELIVERED`/`RETURNED`).
   - Cập nhật Order Status $\rightarrow$ `CANCEL_REQUESTED`.
   - Ghi Transactional Outbox (PostgreSQL) và phát `CancelShipmentCommand` sang topic `shipping-commands`.
2. **Bước 2 (Shipping Service)**:
   - Lắng nghe `shipping-commands` qua `ShippingCommandKafkaListener`.
   - Kiểm tra Redis 7.2 Idempotency (`setIfAbsent` với key `xcare:idempotency:shipping:cancel:{orderId}`, TTL 24h).
   - Gọi `ThirdPartyLogisticsClient` hủy chuyến Ahamove/GHTK.
   - Cập nhật `Shipment` $\rightarrow$ `CANCELLED`.
   - Ghi Transactional Outbox (`shipping_outbox`) và phát `ShipmentCancelledEvent` về topic `order-saga-responses`.
3. **Bước 3 (Order Service - Orchestrator)**:
   - Lắng nghe `order-saga-responses` qua `OrchestratorResponseKafkaListener`.
   - Xác nhận 3PL đã hủy chuyến thành công.
   - Ghi Transactional Outbox và phát `RevertInventoryCommand` sang topic `inventory-commands`.
4. **Bước 4 (Inventory Service)**:
   - Lắng nghe `inventory-commands` qua `InventoryCommandKafkaListener`.
   - Kiểm tra Redis 7.2 Idempotency (`setIfAbsent` với key `xcare:idempotency:inventory:revert:{orderId}`, TTL 24h).
   - **Lexicographical Sort danh sách SKU** để triệt tiêu 100% rủi ro Circular Distributed Deadlock.
   - Acquire **Redisson MultiLock 3.42.0** cho toàn bộ SKU.
   - Hoàn tác số lượng tồn kho trong PostgreSQL: `reserved_quantity -= qty`, `available_quantity += qty`.
   - Release Redisson MultiLock trong khối `finally`.
   - Ghi Transactional Outbox (`fulfillment_outbox`) và phát `InventoryReleasedEvent` về topic `order-saga-responses`.
5. **Bước 5 (Order Service - Orchestrator)**:
   - Lắng nghe `order-saga-responses` qua `OrchestratorResponseKafkaListener`.
   - Cập nhật Order Status $\rightarrow$ `CANCELLED_BY_CUSTOMER`.
   - Ghi **Nhật ký Kiểm toán Đơn thuốc (Prescription Audit Log)** chuẩn Bộ Y Tế.
   - Đóng chuỗi Saga Orchestration thành công tuyệt đối!

#### Danh sách File & Class Java đã thực hiện cho Task 1:
- **`order-service` (:8081)**:
  - `OrderSagaOrchestrator.java`: Central State Machine điều phối toàn bộ chuỗi Saga hủy đơn.
  - `OrderServiceImpl.java`: Tiếp nhận HTTP REST request, kiểm tra đơn, gọi Orchestrator bước 1.
  - `OrchestratorResponseKafkaListener.java`: Lắng nghe topic `order-saga-responses`, phân loại event và kích hoạt bước 3 & bước 5.
  - `CancelShipmentCommand.java` & `ShipmentCancelledEvent.java`: DTOs trao đổi tin.
- **`shipping-service` (:8082)**:
  - `ThirdPartyLogisticsClient.java` & `AhamoveGhtkClientImpl.java`: Adapter gọi API 3PL đối tác (Ahamove/GHTK) với timeout & error handling.
  - `ShippingCommandKafkaListener.java`: Lắng nghe lệnh từ topic `shipping-commands`.
  - `ShippingCancellationService.java`: Kiểm tra Redis Idempotency, hủy vận đơn, ghi outbox và gửi response event.
  - `CancelShipmentCommand.java`: DTO Command nhận từ Orchestrator.
- **`fulfillment-service` (:8083)**:
  - `InventoryCommandKafkaListener.java`: Lắng nghe lệnh từ topic `inventory-commands`.
  - `InventoryReleaseService.java`: Kiểm tra Redis Idempotency, Sort SKU từ điển, Redisson MultiLock 3.42.0, hoàn tác kho, ghi outbox và gửi response event.
  - `RevertInventoryCommand.java`: DTO Command nhận từ Orchestrator.

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

---

## 📌 Task 2: Kịch bản Saga Rollback khi Đóng gói thất bại tại kho (Fulfillment Failed - Hỏng thuốc / Hết hàng trên kệ)

### 1. Bối cảnh nghiệp vụ (Business Scenario)
- **Tình huống**: Đơn thuốc đã qua bước Duyệt đơn thuốc của Dược sĩ và đã tự động Book thành công Shipper Ahamove đến lấy hàng.
- **Sự cố thực tế**: Khi Nhân viên kho / Dược sĩ chi nhánh (`Fulfillment Service :8083`) tiến hành gom hàng và đóng gói, phát hiện **thuốc trong lọ bị nứt vỡ/ẩm mốc hoặc hết hàng thực tế trên kệ vật lý**.
- **Hành động**: Dược sĩ ấn nút **"HỦY ĐÓNG GÓI"** trên giao diện kho (gọi `POST /api/v1/fulfillment/cancel-pack`).
- **Thách thức Saga**: 
  1. Chuyến xe Ahamove đã được đặt trước đó cần được hủy khẩn cấp để tài xế không phải chờ vô ích.
  2. Số lượng tồn kho ảo (Reserved Stock) của đơn hàng này đang bị khóa cần được giải phóng, nhưng thuốc hỏng không được cộng vào `availableQuantity` (phải cách ly hàng hỏng).
  3. Đơn hàng tại `Order Service` phải được chuyển sang trạng thái đặc thù `CANCELLED_OUT_OF_STOCK` kèm lý do chi tiết từ Dược sĩ.

---

### 2. Quy trình 4 Bước Saga Rollback (Task 2 Flow)

```text
[Dược sĩ phát hiện thuốc hỏng/hết hàng trên kệ]
       │
       │ 1. POST /api/v1/fulfillment/cancel-pack
       ▼
┌────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 1: Fulfillment Service (:8083)                                    │
│ - Tiếp nhận yêu cầu HỦY ĐÓNG GÓI từ Dược sĩ kèm lý do & danh sách SKU  │
│ - Ghi nhận sự kiện FULFILLMENT_FAILED vào bảng 'fulfillment_outbox'     │
│ - Transaction ACID Commit vào fulfillment_db!                          │
│ - Outbox Publisher quét (SKIP LOCKED) & bắn: 'fulfillment-events'      │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   │ Kafka Event: FulfillmentFailedEvent
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 2: Shipping Service (:8082)                                       │
│ - Kafka Listener nhận 'fulfillment-events'                             │
│ - Kiểm tra Redis Idempotency: xcare:idempotency:shipping:cancel:{id}   │
│ - Gọi Ahamove API: cancelShipment(trackingCode, "Kho hủy đóng gói")    │
│ - Cập nhật Shipment -> CANCELLED trong shipping_db                     │
│ - Đánh dấu Redis Idempotency = COMPLETED (TTL 24h)                     │
│ - Bắn Kafka Event: 'shipping-events' & 'shipping-cancellation-events'  │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   │ Kafka Event: ShipmentCancelledEvent
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 3: Inventory Service (trong :8083)                                │
│ - Lắng nghe SHIPMENT_CANCELLED mang lý do 'FULFILLMENT_FAILED'         │
│ - Sort SKU theo alphabet -> Acquire Redisson MultiLock                 │
│ - Giải phóng Reserved Quantity: reserved -= qty                        │
│ - Cách ly hàng hỏng (không cộng bừa vào available_quantity)            │
│ - Release Redisson MultiLock trong khối finally                        │
│ - Bắn Kafka Event: 'inventory-events' (sagaType: FULFILLMENT_FAILED)   │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   │ Kafka Event: InventoryReleasedEvent
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 4: Order Service (:8081)                                          │
│ - Kafka Listener nhận 'inventory-events'                               │
│ - Nhận diện sagaType = FULFILLMENT_FAILED_OUT_OF_STOCK                 │
│ - Cập nhật trạng thái Order: CANCELLED_OUT_OF_STOCK                    │
│ - Ghi chép Audit log & Kết thúc hoàn tất Saga Rollback!                │
└────────────────────────────────────────────────────────────────────────┘
```

---

### 3. Danh sách File & Module mới đã triển khai trong Task 2

#### 📦 `fulfillment-service` (:8083)
- `pack/domain/FulfillmentOutboxEvent.java`: Entity lưu sự kiện Outbox cho khâu đóng gói.
- `pack/repository/FulfillmentOutboxEventRepository.java`: Truy vấn `SKIP LOCKED` cho worker nền.
- `pack/dto/CancelPackRequest.java` & `dto/CancelPackResponse.java`: DTO tiếp nhận hủy đóng gói.
- `pack/event/FulfillmentFailedEvent.java`: Payload Kafka event `FULFILLMENT_FAILED`.
- `pack/service/FulfillmentPackService.java` & `impl/FulfillmentPackServiceImpl.java`: Logic lưu Outbox sự cố đóng gói trong 1 Transaction.
- `pack/service/FulfillmentOutboxPublisherService.java`: Worker nền quét định kỳ và đẩy sang Kafka `fulfillment-events`.
- `pack/controller/FulfillmentController.java`: REST API endpoint `POST /api/v1/fulfillment/cancel-pack`.
- `inventory/service/InventoryReleaseService.java`: Bổ sung xử lý phân biệt cách ly thuốc hỏng (`isFulfillmentFailed`), giải phóng reservation và gắn `sagaType: FULFILLMENT_FAILED_OUT_OF_STOCK`.
- `resources/db/changelog/changes/002-create-fulfillment-outbox.xml`: Liquibase migration tạo bảng outbox.

#### 🚚 `shipping-service` (:8082)
- `event/FulfillmentFailedEvent.java`: DTO nhận tin từ topic `fulfillment-events`.
- `listener/FulfillmentFailedKafkaListener.java`: Lắng nghe sự kiện thất bại đóng gói từ Fulfillment Service.
- `service/ShippingCancellationService.java`: Triển khai `processFulfillmentFailure(...)` gọi Ahamove API hủy chuyến và phát `SHIPMENT_CANCELLED`.

#### 📝 `order-service` (:8081)
- `domain/enums/OrderStatus.java`: Bổ sung trạng thái enum `CANCELLED_OUT_OF_STOCK`.
- `event/InventoryReleasedEvent.java`: Bổ sung `cancellationReason` và `sagaType`.
- `service/impl/OrderServiceImpl.java`: Mở rộng `completeOrderCancellation` tự động nhận diện lý do đóng gói thất bại và cập nhật chính xác sang `CANCELLED_OUT_OF_STOCK`.

---

### 4. Hướng dẫn Test Kịch bản Task 2 qua cURL

```bash
# 1. Dược sĩ phát hiện thuốc ẩm mốc khi đóng gói -> Gửi yêu cầu Hủy đóng gói
curl -X POST http://localhost:8083/api/v1/fulfillment/cancel-pack \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "b1a2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d",
    "orderNumber": "XC-20260909-0899",
    "pharmacyHubId": "HUB-HCM-Q1",
    "pharmacistId": "PHARM-MINH-02",
    "reason": "Lọ thuốc kháng sinh Augmentin 1g bị vỡ bao bì chân không khi lấy từ kho",
    "items": [
      {
        "sku": "AUG-1G-TAB",
        "productName": "Augmentin 1g Hộp 14 viên",
        "quantity": 2,
        "damaged": true,
        "defectNote": "Vỏ nhôm bị rách, viên nén đổi màu ẩm"
      }
    ]
  }'
```

**Kết quả quan sát log:**
1. `Fulfillment Service`: Lưu Outbox -> Bắn `FULFILLMENT_FAILED` vào topic `fulfillment-events`.
2. `Shipping Service`: Nhận event -> Gọi hủy đơn Ahamove (mã `AHA-20260909-0899`) -> Bắn `SHIPMENT_CANCELLED`.
3. `Inventory Service`: Nhận `SHIPMENT_CANCELLED` -> Lấy Redisson MultiLock cho `AUG-1G-TAB` -> Giảm `reservedQuantity`, cách ly hàng hỏng -> Bắn `INVENTORY_RELEASED`.
4. `Order Service`: Nhận `INVENTORY_RELEASED` -> Nhận biết sự cố kho -> Chuyển trạng thái đơn thành `CANCELLED_OUT_OF_STOCK` kèm ghi chú Audit kiểm toán!

---

## 📌 Task 3: Orchestration Saga Rollback khi Shipping 3PL Timeout/Failure

### Tóm tắt Siêu Ngắn Gọn (Executive Architecture Summary)

Khi gọi đối tác 3PL (Ahamove/GHTK) gặp lỗi timeout/mạng, hệ thống tự động hoàn tác phân tán theo mô hình **Saga Orchestration** do `Order Service (:8081)` làm Trọng tài:

```
                  ┌────────────────────────────────────────────────────────┐
                  │          ORDER SERVICE (:8081) - ORCHESTRATOR           │
                  └───────────────────────────▲────────────────────────────┘
                                              │  1. SHIPPING_BOOKING_FAILED
                                              │  [Topic: 'shipping-events']
                  ┌───────────────────────────┴────────────────────────────┐
                  │                 SHIPPING SERVICE (:8082)               │
                  │ - Gọi API 3PL (Ahamove/GHTK) thất bại (Timeout/Error)  │
                  │ - @Retry thất bại 3 lần -> Kích hoạt Fallback          │
                  │ - Ghi shipping_outbox và phát Event báo cáo lỗi 3PL    │
                  └────────────────────────────────────────────────────────┘

                  ┌────────────────────────────────────────────────────────┐
                  │          ORDER SERVICE (:8081) - ORCHESTRATOR           │
                  │ - Chuyển trạng thái Order -> REVERTING_INVENTORY       │
                  │ - Ghi outbox_events phát Command sang Inventory Service│
                  └───────────────┬────────────────────────▲───────────────┘
  2. RevertInventoryCommand       │                        │  3. InventoryReleasedEvent
  [Topic: 'inventory-commands']   │                        │  [Topic: 'order-saga-responses']
                                  ▼                        │
                  ┌────────────────────────────────────────┴───────────────┐
                  │           FULFILLMENT / INVENTORY SERVICE (:8083)      │
                  │ - Nhận Command hoàn trả tồn kho từ Orchestrator        │
                  │ - Thực thi Atomic SQL Query trực tiếp trên PostgreSQL  │
                  │   (available += qty, reserved -= qty - Không cần Redis)│
                  │ - Ghi fulfillment_outbox và phát Response Event        │
                  └────────────────────────────────────────────────────────┘

                  ┌────────────────────────────────────────────────────────┐
                  │          ORDER SERVICE (:8081) - ĐÓNG SAGA             │
                  │ - Chuyển trạng thái Order -> ORDER_FAILED_SHIPPING_ERROR│
                  │ - Ghi Prescription Audit Log, hoàn tất Saga Rollback!  │
                  └────────────────────────────────────────────────────────┘
```

#### Quy trình chi tiết 4 Bước:

1. **Shipping Service (:8082)**:
   - Gọi API 3PL qua `@Retry(name = "thirdPartyLogisticsRetry")` (3 lần, Exponential Backoff).
   - Sau 3 lần vẫn lỗi $\rightarrow$ Fallback ghi nhận `shipping_outbox` và bắn Kafka Event `SHIPPING_BOOKING_FAILED` sang topic `shipping-events`.
2. **Order Service (:8081 - Saga Orchestrator)**:
   - Lắng nghe `SHIPPING_BOOKING_FAILED` $\rightarrow$ Đổi trạng thái đơn thành `REVERTING_INVENTORY`.
   - Ghi Transactional Outbox và bắn Kafka Command `REVERT_INVENTORY_COMMAND` sang topic `inventory-commands`.
3. **Inventory Service (:8083 - KHÔNG DÙNG REDIS)**:
   - Lắng nghe `REVERT_INVENTORY_COMMAND` $\rightarrow$ Thực thi **Atomic SQL Query** trực tiếp trên PostgreSQL:
     ```sql
     UPDATE hub_stocks 
     SET available_quantity = available_quantity + :qty, 
         reserved_quantity = GREATEST(0, reserved_quantity - :qty), 
         updated_at = NOW() 
     WHERE hub_id = :hubId AND sku = :sku;
     ```
   - Chống Race Condition bằng row-level lock ngầm định của PostgreSQL.
   - Ghi `fulfillment_outbox` và bắn Kafka Event `INVENTORY_RELEASED` về topic `order-saga-responses`.
4. **Order Service (:8081 - Đóng Saga)**:
   - Lắng nghe `INVENTORY_RELEASED` $\rightarrow$ Chuyển trạng thái đơn sang `ORDER_FAILED_SHIPPING_ERROR`.
   - Ghi Prescription Audit Log, hoàn tất chuỗi Saga Rollback an toàn tuyệt đối.

---

## 📌 Task 4: Resilient Saga Orchestration cho Luồng Điều Chuyển Kho (Stock Transfer) Chịu Lỗi Consumer Crash & Poison Pill ("Manual ACK + Spring Retry + DLQ")

### 1. Bối cảnh nghiệp vụ (Business Scenario)
- **Tình huống**: Hệ thống nhà thuốc XCare cần bổ sung thuốc từ Tổng Kho Trung Tâm (`HUB-CENTRAL-HN`) về Chi Nhánh Nhà Thuốc Quận 1 (`HUB-HCM-Q1`) để đáp ứng đơn thuốc gia tăng.
- **Quy trình Nghiệp vụ**:
  1. Quản lý chuỗi cung ứng (Procurement / Supply Chain Manager) kích hoạt điều chuyển qua API `POST /api/v1/stock-transfers`.
  2. Kho chi nhánh đích (`Fulfillment/Inventory Service :8083`) thực hiện kiểm đếm và cộng số lượng khả dụng (`available_quantity += qty`).
  3. Sau khi kho vật lý hoàn tất, hệ thống kích hoạt BƯỚC 3 để `Order/Catalog Service (:8081)` mở bán Online trở lại cho các SKU này trên Ứng dụng & Web.
- **Thách thức Hạ tầng Phân tán (Task 4 Core Challenges)**:
  1. **Consumer Crash (OOM Crash / SIGKILL ngắt điện đột ngột)**: Khi Server Fulfillment (:8083) hoặc Order (:8081) đang chạy dở dang giao dịch ghi DB thì bị kill. Nếu dùng Auto-Commit, offset đã bị commit trước khi DB ghi xong dẫn đến **mất dữ liệu (Data Loss)**. Khi Container khởi động lại, Kafka Rebalance giao offset uncommitted dẫn đến nguy cơ **cộng đúp tồn kho (Duplicate Stock Inflation)**!
  2. **Tin nhắn độc (Poison Pill)**: Một message mang định dạng JSON dị dạng, sai schema, hoặc vi phạm business rule khiến Consumer ném Exception liên tục. Nếu không có cơ chế Retry giới hạn và DLQ, Consumer sẽ rơi vào vòng lặp vô tận (Infinite Retry Loop), gây **nghẽn tắc toàn bộ Partition hàng đợi (Head-of-Line Blocking)** cho hàng nghìn đơn hàng khác!
  3. **Distributed Deadlock**: Nhiều lượt chuyển kho diễn ra đồng thời giữa các Hub với các SKU hoán đổi chéo nhau (Hub A chuyển SKU-A, SKU-B sang Hub B; đồng thời Hub B chuyển SKU-B, SKU-A sang Hub A). Nếu không sắp xếp thứ tự khóa, sẽ xảy ra **Circular Wait Deadlock** phân tán.

---

### 2. Sơ đồ Kiến trúc & Quy trình 4 Bước Saga Orchestration (Command-Response Star Topology)

```text
[Supply Chain Manager]
       │
       │ 1. POST /api/v1/stock-transfers
       ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ PROCUREMENT SERVICE (:8084) - CENTRAL SAGA ORCHESTRATOR                          │
│ - Tiếp nhận yêu cầu điều chuyển kho (fromHubId -> toHubId)                       │
│ - Lưu StockTransferSaga (Trạng thái: TRANSFER_REQUESTED) vào procurement_db      │
│ - Ghi Transactional Outbox: 'procurement_outbox_events'                          │
│ - [BƯỚC 1] Bắn Kafka Command: 'inventory-commands' (Key: transferId)            │
└────────────────────────────────────────┬─────────────────────────────────────────┘
                                         │
                                         │ Kafka Command: TransferStockCommand
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ FULFILLMENT / INVENTORY SERVICE (:8083) - PARTICIPANT 1                          │
│ - Kafka Listener Container: Tắt Auto-Commit, bật AckMode.MANUAL_IMMEDIATE        │
│ - [BƯỚC 2.1] Kiểm tra Redis Idempotency: xcare:idempotency:transfer:{id}:{sku}  │
│   (setIfAbsent TTL 24h). Nếu trùng lặp do Rebalance -> Bỏ qua an toàn!           │
│ - [BƯỚC 2.2] Sắp xếp SKU theo Alphabet (Lexicographical Sort) -> Triệt tiêu     │
│   nguy cơ Distributed Deadlock khi chạy concurrent!                              │
│ - [BƯỚC 2.3] Giữ Redisson MultiLock 3.42.0 cho danh sách SKU tại Hub đích       │
│ - [BƯỚC 2.4] Atomic DB Update PostgreSQL: available_quantity += qty              │
│ - [BƯỚC 2.5] Ghi Transactional Outbox: fulfillment_outbox (STOCK_TRANSFERRED)   │
│ - [BƯỚC 2.6] Release MultiLock trong khối finally                                │
│ - [BƯỚC 2.7] Gửi Manual ACK: ack.acknowledge() lên Kafka Broker                  │
│ - Bắn Kafka Event: 'inventory-saga-responses' (Key: transferId)                  │
└────────────────────────────────────────┬─────────────────────────────────────────┘
                                         │
                                         │ Kafka Event: StockTransferredEvent
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ PROCUREMENT SERVICE (:8084) - CENTRAL SAGA ORCHESTRATOR                          │
│ - Nhận StockTransferredEvent từ topic 'inventory-saga-responses'                 │
│ - Cập nhật trạng thái Saga sang INVENTORY_UPDATED trong procurement_db           │
│ - Ghi Transactional Outbox: 'procurement_outbox_events'                          │
│ - [BƯỚC 3] Bắn Kafka Command: 'catalog-commands' (Key: transferId)               │
└────────────────────────────────────────┬─────────────────────────────────────────┘
                                         │
                                         │ Kafka Command: SyncOnlineStockCommand
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ ORDER / CATALOG SERVICE (:8081) - PARTICIPANT 2                                  │
│ - Kafka Listener Container: Tắt Auto-Commit, bật AckMode.MANUAL_IMMEDIATE        │
│ - [BƯỚC 4.1] Kiểm tra Redis Idempotency: xcare:idempotency:catalog:sync:{id}:{sku}
│ - [BƯỚC 4.2] Mở bán lại sản phẩm trên Kênh Online & cập nhật trạng thái Catalog  │
│ - [BƯỚC 4.3] Ghi Transactional Outbox: outbox_events (ONLINE_STOCK_SYNCED)       │
│ - [BƯỚC 4.4] Gửi Manual ACK: ack.acknowledge()                                   │
│ - Bắn Kafka Event: 'inventory-saga-responses' (Key: transferId)                  │
└────────────────────────────────────────┬─────────────────────────────────────────┘
                                         │
                                         │ Kafka Event: OnlineStockSyncedEvent
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ PROCUREMENT SERVICE (:8084) - CHỐT HOÀN TẤT SAGA                                │
│ - Nhận OnlineStockSyncedEvent từ 'inventory-saga-responses'                      │
│ - Chốt trạng thái Saga: COMPLETED                                                │
│ - Ghi Prescription/Stock Audit Trail -> Toàn bộ luồng Saga kết thúc thành công! │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

### 3. Cơ chế Bảo vệ Consumer Cấp Hạ tầng (Task 4 Core Infrastructure Guard)

#### 🛡️ 1. Bộ ba "Manual ACK + Spring Retry + Dead-Letter Queue (DLQ)"
- **Manual ACK (`AckMode.MANUAL_IMMEDIATE`)**:
  - `KafkaConsumerConfig.java` tắt `enable.auto.commit = false`.
  - Offset chỉ được commit lên Kafka broker khi và chỉ khi **DB Transaction đã commit thành công** và sự kiện Outbox đã ghi nhận an toàn.
  - Nếu JVM bị OOM Crash (`kill -9`), Kafka broker không nhận được ACK $\rightarrow$ Sau timeout, Kafka rebalance giao lại tin nhắn cho Consumer sống khác.
- **Spring Retry (`@RetryableTopic`)**:
  - Cấu hình `@RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0), dltTopicSuffix = "-DLQ")`.
  - Tự động thử lại 3 lần với Exponential Backoff (Lần 1: 1000ms, Lần 2: 2000ms, Lần 3: 4000ms) trước các lỗi Transient (mất kết nối tạm thời, lock timeout).
- **Dead-Letter Queue (`@DltHandler`)**:
  - Khi đã thử lại hết 3 lần (Exhausted), tin nhắn độc (Poison Pill) tự động được đẩy sang Topic DLQ riêng biệt:
    + `inventory-commands` $\rightarrow$ `inventory-commands-DLQ`
    + `catalog-commands` $\rightarrow$ `catalog-commands-DLQ`
  - Phương thức `@DltHandler` tiếp nhận tin nhắn độc, ghi nhận log cảnh báo SRE/Audit và chủ động gọi `ack.acknowledge()` để **triệt tiêu hoàn toàn hiện tượng Head-of-Line Blocking** (hàng đợi không bao giờ bị tắc nghẽn).

#### 🔒 2. Redis Idempotency & Redisson MultiLock (Lexicographical Sort)
- **Redis Idempotency Guard**:
  - Sử dụng Redis 7.2 lệnh `setIfAbsent` với key `xcare:idempotency:transfer:{transferId}:{sku}` (TTL 24h).
  - Khi Server bị Crash và Kafka Rebalance đọc lại uncommitted offset, Consumer kiểm tra Redis thấy key đã `COMPLETED` sẽ **bỏ qua ngay lập tức**, ngăn chặn 100% việc cộng thừa tồn kho.
- **Lexicographical Sort (Sắp xếp từ điển)**:
  - Trước khi yêu cầu `Redisson MultiLock`, danh sách SKU bắt buộc phải được sort theo thứ tự từ điển (A $\rightarrow$ Z).
  - Đảm bảo tất cả các thread/service trên toàn cụm phân tán đều xin khóa theo cùng một thứ tự duy nhất, loại bỏ hoàn toàn hiện tượng **Circular Wait** (nguyên nhân gây Distributed Deadlock).

---

### 4. Danh sách File & Module mới đã triển khai trong Task 4

#### 🏢 `procurement-service` (:8084) - *Saga Orchestrator mới*
- `ProcurementServiceApplication.java`: Main Spring Boot Application của dịch vụ điều phối cung ứng.
- `config/KafkaConsumerConfig.java`: Cấu hình Kafka Consumer Container với `AckMode.MANUAL_IMMEDIATE`.
- `domain/enums/TransferSagaStatus.java`: Trạng thái Saga (`TRANSFER_REQUESTED`, `INVENTORY_UPDATED`, `COMPLETED`, `FAILED`).
- `domain/enums/OutboxStatus.java`: Trạng thái Transactional Outbox.
- `domain/entity/StockTransferSaga.java`: Entity JPA quản lý State Machine điều chuyển kho.
- `domain/entity/ProcurementOutboxEvent.java`: Entity JPA Transactional Outbox.
- `repository/StockTransferSagaRepository.java`: Repository tra cứu Saga theo `transferId`.
- `repository/ProcurementOutboxEventRepository.java`: Repository Outbox hỗ trợ `SKIP LOCKED`.
- `dto/CreateStockTransferRequest.java` & `dto/StockTransferResponse.java`: DTO API tiếp nhận điều chuyển kho.
- `event/TransferStockCommand.java`, `StockTransferredEvent.java`, `SyncOnlineStockCommand.java`, `OnlineStockSyncedEvent.java`: Các message DTO truyền tải qua Kafka.
- `saga/StockTransferSagaOrchestrator.java`: Xương sống điều phối State Machine 4 bước (BƯỚC 1 phát Command $\rightarrow$ BƯỚC 3 nhận Response & phát Sync $\rightarrow$ BƯỚC 4 chốt Saga).
- `listener/ProcurementResponseKafkaListener.java`: Lắng nghe Topic `inventory-saga-responses` kèm Manual ACK.
- `controller/StockTransferController.java`: REST API endpoints `POST /api/v1/stock-transfers` & `GET /api/v1/stock-transfers/{transferId}`.

#### 📦 `fulfillment-service` (:8083) - *Inventory Participant*
- `config/KafkaConsumerConfig.java`: Cấu hình Kafka Listener Container với `AckMode.MANUAL_IMMEDIATE` và tắt Auto-Commit.
- `inventory/event/TransferStockCommand.java` & `inventory/event/StockTransferredEvent.java`: DTOs nhận lệnh và phát event.
- `inventory/service/StockTransferService.java`: Triển khai Redis Idempotency Guard (key `xcare:idempotency:transfer:...`), sắp xếp SKU từ điển, `Redisson MultiLock`, cộng tồn kho atomic PostgreSQL, ghi `fulfillment_outbox`, và giải phóng lock an toàn trong `finally`.
- `inventory/listener/InventoryCommandKafkaListener.java`: Kafka Listener trên topic `inventory-commands` trang bị `@RetryableTopic` (3 lần, Exponential Backoff), Manual `ack.acknowledge()`, và `@DltHandler` cô lập Poison Pill vào `inventory-commands-DLQ`.

#### 🛍️ `order-service` (:8081) - *Catalog Online Participant*
- `config/KafkaConsumerConfig.java`: Cấu hình Kafka Listener Container với `AckMode.MANUAL_IMMEDIATE` và tắt Auto-Commit.
- `event/SyncOnlineStockCommand.java` & `event/OnlineStockSyncedEvent.java`: DTOs nhận lệnh đồng bộ và phát event hoàn tất.
- `service/CatalogStockSyncService.java`: Xử lý Redis Idempotency (key `xcare:idempotency:catalog:sync:...`), mở bán sản phẩm Online, ghi Outbox vào `orders_db`, phát event sang `inventory-saga-responses`.
- `listener/CatalogCommandKafkaListener.java`: Kafka Listener trên topic `catalog-commands` trang bị `@RetryableTopic`, Manual `ack.acknowledge()`, và `@DltHandler` chuyển tin độc sang `catalog-commands-DLQ`.

#### 🌐 Hạ tầng & Gateway
- `init-scripts/01-init-databases.sql`: Bổ sung khởi tạo `procurement_db` và phân quyền cho `postgres`.
- `api-gateway/src/main/resources/application.yml`: Bổ sung route `/api/v1/stock-transfers/**` chuyển tiếp sang `:8084`.
- `pom.xml`: Khai báo module con `procurement-service` trong kiến trúc Maven Multi-Module.

---

### 5. Hướng dẫn Test Kịch bản Task 4 qua cURL & Giả lập Sự cố Crash `kill -9`

#### 🧪 Kịch bản 1: Kích hoạt Luồng Điều Chuyển Kho Thành Công (Happy Path)

```bash
# 1. Bắn cURL kích hoạt điều chuyển 50 hộp Augmentin & 30 lọ Panadol từ Tổng Kho về Quận 1
curl -X POST http://localhost:8084/api/v1/stock-transfers \
  -H "Content-Type: application/json" \
  -d '{
    "fromHubId": "HUB-CENTRAL-HN",
    "toHubId": "HUB-HCM-Q1",
    "reason": "Điều chuyển bổ sung thuốc kháng sinh & hạ sốt phục vụ mùa dịch tại TP.HCM",
    "requestedBy": "SUPPLY-LE-THI-MAI",
    "items": [
      {
        "sku": "AUG-1G-TAB",
        "productName": "Augmentin 1g Hộp 14 viên",
        "quantity": 50
      },
      {
        "sku": "PAN-500-SYR",
        "productName": "Panadol Hạ Sốt Trẻ Em Chai 60ml",
        "quantity": 30
      }
    ]
  }'

# 2. Kiểm tra tiến trình và trạng thái State Machine của Saga
# (Thay TRF-172599... bằng transferId trả về từ response trên)
curl -X GET http://localhost:8084/api/v1/stock-transfers/TRF-1725998822001
```

**Quan sát Luồng Log Thực tế:**
1. **Procurement Service (:8084)**: Lưu Saga trạng thái `TRANSFER_REQUESTED`, bắn `TransferStockCommand` sang topic `inventory-commands`.
2. **Fulfillment Service (:8083)**:
   - Nhận command $\rightarrow$ Khởi tạo Redis Idempotency `xcare:idempotency:transfer:TRF-...:AUG-1G-TAB` = `PROCESSING`.
   - Sắp xếp SKU: `["AUG-1G-TAB", "PAN-500-SYR"]`.
   - Giữ Redisson MultiLock cho 2 SKU $\rightarrow$ Cộng tồn kho trong PostgreSQL.
   - Set Redis Idempotency = `COMPLETED` $\rightarrow$ Release Redisson MultiLock trong `finally`.
   - Bắn `StockTransferredEvent` sang `inventory-saga-responses` $\rightarrow$ Gửi Manual ACK `ack.acknowledge()`.
3. **Procurement Service (:8084)**:
   - Nhận `StockTransferredEvent` $\rightarrow$ Chuyển Saga sang `INVENTORY_UPDATED`.
   - Bắn `SyncOnlineStockCommand` sang topic `catalog-commands`.
4. **Order / Catalog Service (:8081)**:
   - Nhận command $\rightarrow$ Kiểm tra Redis Idempotency $\rightarrow$ Mở bán lại SKU Online.
   - Ghi Outbox và bắn `OnlineStockSyncedEvent` sang `inventory-saga-responses` $\rightarrow$ Gửi Manual ACK.
5. **Procurement Service (:8084)**:
   - Nhận `OnlineStockSyncedEvent` $\rightarrow$ Chuyển Saga sang `COMPLETED` (Hoàn tất 100%).

---

#### 💥 Kịch bản 2: Giả lập Sự cố Consumer Crash (`kill -9`) & Kiểm chứng Redis Idempotency Guard

**Mục tiêu**: Chứng minh rằng khi Server Fulfillment Service bị Crash ngắt điện giữa chừng, tin nhắn không bị mất (nhờ `MANUAL_IMMEDIATE`) và khi khởi động lại, tồn kho **không bao giờ bị cộng thừa 2 lần** (nhờ Redis Idempotency Guard).

```bash
# Bước 1: Tìm PID của Fulfillment Service (:8083)
PID=$(lsof -ti:8083)
echo "Fulfillment Service PID: $PID"

# Bước 2: Bắn cURL điều chuyển kho
curl -X POST http://localhost:8084/api/v1/stock-transfers \
  -H "Content-Type: application/json" \
  -d '{
    "fromHubId": "HUB-CENTRAL-HN",
    "toHubId": "HUB-HCM-Q1",
    "reason": "Stress test Consumer Crash",
    "requestedBy": "QA-ENGINEER",
    "items": [{"sku": "AUG-1G-TAB", "productName": "Augmentin 1g", "quantity": 10}]
  }' &

# Bước 3: NGAY LẬP TỨC SIGKILL SERVER KHO (Giả lập OOM / Sập nguồn điện)
kill -9 $PID

# Bước 4: Kiểm tra trạng thái Kafka Consumer Offset qua Kafka UI (http://localhost:8090)
# Kết quả quan sát: Offset trên topic 'inventory-commands' CHƯA ĐƯỢC COMMIT vì Consumer chưa kịp chạy ack.acknowledge()!

# Bước 5: Khởi động lại Fulfillment Service (:8083)
# cd fulfillment-service && ./mvnw spring-boot:run

# Bước 6: Quan sát Log khi Service khởi động lại
# Kafka Broker tự động Rebalance và giao lại offset uncommitted cho Fulfillment Service.
# Consumer nhận lại message cũ -> Kiểm tra Redis Idempotency:
# "[STOCK-TRANSFER-IDEMPOTENT-HIT] SKU [AUG-1G-TAB] trong transfer [...] đã được xử lý (Status: COMPLETED). Bỏ qua lặp!"
# -> Bỏ qua update DB -> Gọi ack.acknowledge() -> Tồn kho được bảo toàn chính xác tuyệt đối!
```

---

#### ☠️ Kịch bản 3: Giả lập Tin Nhắn Độc (Poison Pill) & Kiểm chứng DLQ

**Mục tiêu**: Kiểm chứng Spring Retry thử lại 3 lần với Exponential Backoff, sau đó tự động cách ly tin nhắn độc sang `inventory-commands-DLQ` mà không làm tắc nghẽn hàng đợi (No Head-of-Line Blocking).

```bash
# Gửi trực tiếp 1 bản tin JSON dị dạng (Poison Pill) vào Topic 'inventory-commands' qua kafka-console-producer
docker exec -i xcare-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic inventory-commands <<EOF
{"invalid_json_schema": true, "corrupted_payload": "%%%POISON_PILL%%%"}
EOF
```

**Kết quả quan sát Log & DLQ:**
1. **Lần 1 (T = 0s)**: Consumer ném ngoại lệ $\rightarrow$ Kích hoạt Retry Lần 1 (delay 1000ms).
2. **Lần 2 (T = 1s)**: Consumer ném ngoại lệ $\rightarrow$ Kích hoạt Retry Lần 2 (delay 2000ms).
3. **Lần 3 (T = 3s)**: Consumer ném ngoại lệ $\rightarrow$ Kích hoạt Retry Lần 3 (delay 4000ms).
4. **DLQ Routing**: Thử lại 3 lần thất bại $\rightarrow$ Message tự động chuyển vào `inventory-commands-DLQ`.
5. **@DltHandler**: Tiếp nhận Poison Pill, ghi log:
   `"[INVENTORY-DLQ-HANDLER][CẢNH BÁO NGUY CẤP] Tin nhắn độc (Poison Pill) đã được cách ly vào DLQ topic [inventory-commands-DLQ]"`
6. `@DltHandler` chủ động gọi `ack.acknowledge()` $\rightarrow$ Hàng đợi Kafka được thông suốt, các đơn hàng hợp lệ tiếp theo được xử lý bình thường mà không bị nghẽn!



