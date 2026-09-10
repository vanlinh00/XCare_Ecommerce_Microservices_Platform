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

1. **Shipping Service (:8082)**:
   - Gọi API 3PL qua `@Retry(name = "thirdPartyLogisticsRetry")` (3 lần, Exponential Backoff).
   - Sau 3 lần vẫn lỗi $\rightarrow$ Fallback ghi nhận `shipping_outbox` và bắn Kafka Event `SHIPPING_BOOKING_FAILED`.
2. **Order Service (:8081 - Saga Orchestrator)**:
   - Lắng nghe `SHIPPING_BOOKING_FAILED` $\rightarrow$ Đổi trạng thái đơn thành `REVERTING_INVENTORY`.
   - Ghi Transactional Outbox và bắn Kafka Command `REVERT_INVENTORY_COMMAND`.
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
   - Ghi `fulfillment_outbox` và bắn Kafka Event `INVENTORY_RELEASED`.
4. **Order Service (:8081 - Đóng Saga)**:
   - Lắng nghe `INVENTORY_RELEASED` $\rightarrow$ Chuyển trạng thái đơn sang `ORDER_FAILED_SHIPPING_ERROR`.
   - Ghi Audit Log, hoàn tất chuỗi Saga Rollback an toàn tuyệt đối.


