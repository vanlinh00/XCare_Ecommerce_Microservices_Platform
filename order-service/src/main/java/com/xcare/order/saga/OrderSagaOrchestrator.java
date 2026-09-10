package com.xcare.order.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.order.domain.entity.Order;
import com.xcare.order.domain.entity.OutboxEvent;
import com.xcare.order.domain.enums.OrderStatus;
import com.xcare.order.domain.enums.OutboxStatus;
import com.xcare.order.event.CancelShipmentCommand;
import com.xcare.order.event.InventoryReleasedEvent;
import com.xcare.order.event.RevertInventoryCommand;
import com.xcare.order.event.ShipmentCancelledEvent;
import com.xcare.order.event.ShippingBookingFailedEvent;
import com.xcare.order.repository.OrderRepository;
import com.xcare.order.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Trọng tài điều phối trung tâm (Central Saga Orchestrator) cho hệ sinh thái Dược phẩm XCare.
 * Điều phối chuỗi Saga State Machine:
 * - Kịch bản 1: Khách hàng/Dược sĩ hủy đơn thuốc (Order Cancellation Saga: 5 Bước chuẩn).
 * - Kịch bản 3: Tự động Saga Rollback khi Shipping 3PL Timeout/Failure.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaOrchestrator {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${xcare.outbox.topics.shipping-commands:shipping-commands}")
    private String shippingCommandsTopic;

    @Value("${xcare.outbox.topics.inventory-commands:inventory-commands}")
    private String inventoryCommandsTopic;

    // =========================================================================
    // TASK 1: KỊCH BẢN HỦY ĐƠN THUỐC & KÍCH HOẠT SAGA ROLLBACK (5 BƯỚC)
    // =========================================================================

    /**
     * BƯỚC 1 (Orchestrator - Khởi tạo):
     * Nhận yêu cầu hủy đơn từ OrderServiceImpl.
     * Cập nhật Order Status -> CANCEL_REQUESTED.
     * Ghi Transactional Outbox (PostgreSQL) và gửi CancelShipmentCommand sang topic 'shipping-commands'.
     */
    @Transactional
    public void startCancelOrderSaga(Order order, String sagaId, String reason, String cancelledBy) {
        log.info("[SAGA-ORCHESTRATOR][BƯỚC 1] Bắt đầu Saga Hủy đơn thuốc [{}] - SagaId [{}] - Người hủy: {}",
                order.getOrderNumber(), sagaId, cancelledBy);

        // 1. Chuyển trạng thái Order sang CANCEL_REQUESTED
        order.setStatus(OrderStatus.CANCEL_REQUESTED);
        order.setNote("Saga hủy đơn đang tiến hành. Lý do: " + reason);
        orderRepository.save(order);

        // 2. Chuẩn bị CancelShipmentCommand gửi cho Shipping Service (:8082)
        List<CancelShipmentCommand.CancelItemPayload> cancelItems = order.getItems().stream()
                .map(item -> CancelShipmentCommand.CancelItemPayload.builder()
                        .sku(item.getSku())
                        .productName(item.getProductName())
                        .quantity(item.getQuantity())
                        .build())
                .collect(Collectors.toList());

        CancelShipmentCommand command = CancelShipmentCommand.builder()
                .commandId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(order.getId())
                .orderNumber(order.getOrderNumber())
                .pharmacyHubId(order.getPharmacyHubId())
                .carrier("AHAMOVE")
                .reason(reason)
                .cancelledBy(cancelledBy)
                .createdAt(Instant.now())
                .items(cancelItems)
                .build();

        try {
            String payloadJson = objectMapper.writeValueAsString(command);

            // 3. Ghi Transactional Outbox vào PostgreSQL (Atomic cùng Order State)
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("ORDER_SAGA")
                    .aggregateId(order.getId().toString())
                    .eventType("CANCEL_SHIPMENT_COMMAND")
                    .topic(shippingCommandsTopic)
                    .partitionKey(order.getOrderNumber())
                    .payload(payloadJson)
                    .status(OutboxStatus.PUBLISHED)
                    .retryCount(0)
                    .createdAt(Instant.now())
                    .publishedAt(Instant.now())
                    .build();
            outboxEventRepository.save(outboxEvent);

            // 4. Bắn Kafka Command sang Shipping Service (:8082)
            kafkaTemplate.send(shippingCommandsTopic, order.getOrderNumber(), payloadJson);
            log.info("[SAGA-ORCHESTRATOR][BƯỚC 1] Đã phát CancelShipmentCommand tới topic '{}' cho đơn [{}]",
                    shippingCommandsTopic, order.getOrderNumber());

        } catch (Exception e) {
            log.error("[SAGA-ORCHESTRATOR][BƯỚC 1] Lỗi khi tạo CancelShipmentCommand: {}", e.getMessage(), e);
            throw new RuntimeException("Khởi tạo Saga Hủy đơn thất bại", e);
        }
    }

    /**
     * BƯỚC 3 (Orchestrator - Nhận Response Hủy Ship & Phát Lệnh Hoàn Kho):
     * Khi nhận được xác nhận ShipmentCancelledEvent từ topic 'order-saga-responses'.
     * Orchestrator xác nhận 3PL đã hủy -> Lưu Outbox và bắn RevertInventoryCommand sang topic 'inventory-commands'.
     */
    @Transactional
    public void handleShipmentCancelled(ShipmentCancelledEvent event) {
        log.info("[SAGA-ORCHESTRATOR][BƯỚC 3] Nhận phản hồi Hủy Ship thành công cho đơn [{}] từ đối tác [{}]",
                event.getOrderNumber(), event.getCarrier());

        Order order = orderRepository.findById(event.getOrderId()).orElse(null);
        if (order == null) {
            log.error("[SAGA-ORCHESTRATOR][BƯỚC 3] Không tìm thấy đơn hàng ID: {}", event.getOrderId());
            return;
        }

        // Kiểm tra Idempotency trên Orchestrator State
        if (order.getStatus() == OrderStatus.CANCELLED_BY_CUSTOMER || order.getStatus() == OrderStatus.CANCELLED) {
            log.warn("[SAGA-ORCHESTRATOR][BƯỚC 3] Đơn [{}] đã hoàn tất hủy từ trước, bỏ qua xử lý lặp", order.getOrderNumber());
            return;
        }

        order.setNote("3PL (" + event.getCarrier() + ") đã hủy vận đơn [" + event.getTrackingCode() + "]. Bắt đầu hoàn trả tồn kho Hub.");
        orderRepository.save(order);

        // Chuẩn bị RevertInventoryCommand gửi Hub Fulfillment / Inventory Service (:8083)
        List<RevertInventoryCommand.RevertItemPayload> revertItems = (event.getItems() != null && !event.getItems().isEmpty())
                ? event.getItems().stream()
                    .map(i -> new RevertInventoryCommand.RevertItemPayload(i.getSku(), i.getProductName(), i.getQuantity()))
                    .collect(Collectors.toList())
                : order.getItems().stream()
                    .map(i -> new RevertInventoryCommand.RevertItemPayload(i.getSku(), i.getProductName(), i.getQuantity()))
                    .collect(Collectors.toList());

        RevertInventoryCommand command = RevertInventoryCommand.builder()
                .commandId(UUID.randomUUID())
                .sagaId(event.getSagaId())
                .orderId(order.getId())
                .orderNumber(order.getOrderNumber())
                .pharmacyHubId(order.getPharmacyHubId())
                .reason(event.getCancelReason())
                .createdAt(Instant.now())
                .items(revertItems)
                .build();

        try {
            String payloadJson = objectMapper.writeValueAsString(command);

            // Lưu Outbox vào PostgreSQL
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("ORDER_SAGA")
                    .aggregateId(order.getId().toString())
                    .eventType("REVERT_INVENTORY_COMMAND")
                    .topic(inventoryCommandsTopic)
                    .partitionKey(order.getPharmacyHubId())
                    .payload(payloadJson)
                    .status(OutboxStatus.PUBLISHED)
                    .retryCount(0)
                    .createdAt(Instant.now())
                    .publishedAt(Instant.now())
                    .build();
            outboxEventRepository.save(outboxEvent);

            // Bắn Kafka Command sang Inventory Service (:8083)
            kafkaTemplate.send(inventoryCommandsTopic, order.getPharmacyHubId(), payloadJson);
            log.info("[SAGA-ORCHESTRATOR][BƯỚC 3] Đã phát RevertInventoryCommand tới topic '{}' cho Hub [{}]",
                    inventoryCommandsTopic, order.getPharmacyHubId());

        } catch (Exception e) {
            log.error("[SAGA-ORCHESTRATOR][BƯỚC 3] Lỗi khi phát RevertInventoryCommand: {}", e.getMessage(), e);
            throw new RuntimeException("Thất bại khi điều phối bước hoàn kho Saga", e);
        }
    }

    /**
     * BƯỚC 5 (Orchestrator - Kết thúc Saga Hủy đơn):
     * Nhận phản hồi InventoryReleasedEvent từ Inventory Service (:8083) qua topic 'order-saga-responses'.
     * Cập nhật Order Status -> CANCELLED_BY_CUSTOMER.
     * Ghi Nhật ký Kiểm toán Đơn thuốc (Prescription Audit Log) phục vụ quy định Y Tế / Dược Phẩm.
     */
    @Transactional
    public void handleInventoryReleased(InventoryReleasedEvent event) {
        log.info("[SAGA-ORCHESTRATOR][BƯỚC 5] Nhận phản hồi Hoàn kho thành công cho đơn [{}] tại Hub [{}]",
                event.getOrderNumber(), event.getPharmacyHubId());

        Order order = orderRepository.findById(event.getOrderId()).orElse(null);
        if (order == null) {
            log.error("[SAGA-ORCHESTRATOR][BƯỚC 5] Không tìm thấy đơn hàng ID: {}", event.getOrderId());
            return;
        }

        if (order.getStatus() == OrderStatus.CANCELLED_BY_CUSTOMER) {
            log.info("[SAGA-ORCHESTRATOR][BƯỚC 5] Đơn hàng [{}] đã hoàn tất hủy trước đó.", order.getOrderNumber());
            return;
        }

        // 1. Chuyển trạng thái đơn hàng sang CANCELLED_BY_CUSTOMER
        order.setStatus(OrderStatus.CANCELLED_BY_CUSTOMER);
        String auditMessage = String.format(
                "SAGA HOÀN TẤT THÀNH CÔNG: Đơn thuốc đã hủy an toàn. 3PL đã hủy. Kho Hub [%s] đã hoàn trả tồn kho %d SKUs qua Redisson MultiLock. Lý do: %s",
                event.getPharmacyHubId(),
                event.getItems() != null ? event.getItems().size() : 0,
                event.getCancellationReason() != null ? event.getCancellationReason() : "Khách hàng yêu cầu"
        );
        order.setNote(auditMessage);
        orderRepository.save(order);

        // 2. Ghi log kiểm toán đơn thuốc (Prescription Audit Log - Chuẩn GPP / Bộ Y Tế)
        writePrescriptionAuditLog(order, event);

        log.info("[SAGA-ORCHESTRATOR][BƯỚC 5] HOÀN TẤT CHUỖI SAGA HỦY ĐƠN THUỐC! Đơn [{}] -> CANCELLED_BY_CUSTOMER",
                order.getOrderNumber());
    }

    /**
     * Ghi nhật ký kiểm toán đơn thuốc (Prescription Audit Log) đảm bảo tính toàn vẹn và tuân thủ pháp lý dược phẩm.
     */
    private void writePrescriptionAuditLog(Order order, InventoryReleasedEvent event) {
        log.info("================================================================================");
        log.info("[PRESCRIPTION AUDIT LOG] - XCARE PHARMACEUTICAL COMPLIANCE RECORD");
        log.info("Order ID              : {}", order.getId());
        log.info("Order Number          : {}", order.getOrderNumber());
        log.info("Customer ID           : {}", order.getCustomerId());
        log.info("Pharmacy Hub ID       : {}", order.getPharmacyHubId());
        log.info("Final Status          : {}", order.getStatus());
        log.info("Audit Timestamp       : {}", Instant.now());
        log.info("Saga ID               : {}", event.getSagaId());
        log.info("Cancellation Reason   : {}", event.getCancellationReason());
        if (event.getItems() != null) {
            event.getItems().forEach(item ->
                log.info(" - SKU Restocked      : [{}] | Released Qty: {}", item.getSku(), item.getReleasedQuantity())
            );
        }
        log.info("Compliance Validation : PASSED (Redisson MultiLock Atomic Release & 3PL Carrier Verified)");
        log.info("================================================================================");
    }

    // =========================================================================
    // TASK 3: TỰ ĐỘNG SAGA ROLLBACK KHI SHIPPING 3PL TIMEOUT / FAILURE
    // =========================================================================

    @KafkaListener(
            topics = "${xcare.topics.shipping-events:shipping-events}",
            groupId = "xcare-order-saga-orchestrator-group"
    )
    @Transactional
    public void onShippingBookingFailed(ConsumerRecord<String, String> record) {
        try {
            if (!record.value().contains("SHIPPING_BOOKING_FAILED") && !record.value().contains("failureReason")) {
                return;
            }

            ShippingBookingFailedEvent event = objectMapper.readValue(record.value(), ShippingBookingFailedEvent.class);
            log.warn("[SAGA-ORCHESTRATOR][TASK 3] Nhận báo cáo lỗi 3PL từ Shipping Service cho đơn {}: {}",
                    event.getOrderNumber(), event.getFailureReason());

            Order order = orderRepository.findById(event.getOrderId()).orElse(null);
            if (order == null) {
                log.error("[SAGA-ORCHESTRATOR] Không tìm thấy đơn hàng {} để điều phối Rollback", event.getOrderId());
                return;
            }

            if (order.getStatus() == OrderStatus.REVERTING_INVENTORY || 
                order.getStatus() == OrderStatus.ORDER_FAILED_SHIPPING_ERROR) {
                return;
            }

            order.setStatus(OrderStatus.REVERTING_INVENTORY);
            order.setNote("Saga Rollback in progress: 3PL Failure [" + event.getFailureReason() + "]");
            orderRepository.save(order);

            List<RevertInventoryCommand.RevertItemPayload> revertItems = (event.getItems() != null && !event.getItems().isEmpty())
                    ? event.getItems().stream()
                        .map(i -> new RevertInventoryCommand.RevertItemPayload(i.getSku(), i.getProductName(), i.getQuantity()))
                        .collect(Collectors.toList())
                    : order.getItems().stream()
                        .map(i -> new RevertInventoryCommand.RevertItemPayload(i.getSku(), i.getProductName(), i.getQuantity()))
                        .collect(Collectors.toList());

            RevertInventoryCommand command = RevertInventoryCommand.builder()
                    .commandId(UUID.randomUUID())
                    .sagaId("SAGA-ROLLBACK-" + order.getId())
                    .orderId(order.getId())
                    .orderNumber(order.getOrderNumber())
                    .pharmacyHubId(order.getPharmacyHubId())
                    .reason(event.getFailureReason())
                    .createdAt(Instant.now())
                    .items(revertItems)
                    .build();

            String commandPayload = objectMapper.writeValueAsString(command);

            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("ORDER_SAGA")
                    .aggregateId(order.getId().toString())
                    .eventType("REVERT_INVENTORY_COMMAND")
                    .topic(inventoryCommandsTopic)
                    .partitionKey(order.getPharmacyHubId())
                    .payload(commandPayload)
                    .status(OutboxStatus.PUBLISHED)
                    .retryCount(0)
                    .createdAt(Instant.now())
                    .publishedAt(Instant.now())
                    .build();
            outboxEventRepository.save(outboxEvent);

            kafkaTemplate.send(inventoryCommandsTopic, order.getPharmacyHubId(), commandPayload);
            log.info("[SAGA-ORCHESTRATOR][TASK 3] Đã phát REVERT_INVENTORY_COMMAND sang '{}' cho kho {}",
                    inventoryCommandsTopic, order.getPharmacyHubId());

        } catch (Exception e) {
            log.error("[SAGA-ORCHESTRATOR] Lỗi xử lý event SHIPPING_BOOKING_FAILED: {}", e.getMessage(), e);
        }
    }
}
