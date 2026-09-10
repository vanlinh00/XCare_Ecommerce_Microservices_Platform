package com.xcare.order.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.order.domain.entity.Order;
import com.xcare.order.domain.entity.OutboxEvent;
import com.xcare.order.domain.enums.OrderStatus;
import com.xcare.order.domain.enums.OutboxStatus;
import com.xcare.order.event.InventoryReleasedEvent;
import com.xcare.order.event.RevertInventoryCommand;
import com.xcare.order.event.ShippingBookingFailedEvent;
import com.xcare.order.repository.OrderRepository;
import com.xcare.order.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Senior Implementation: Saga Orchestrator for Order Processing.
 * Manages the compensation flow when external 3PL Shipping API fails.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaOrchestrator {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public static final String TOPIC_INVENTORY_COMMANDS = "inventory-commands";

    /**
     * Bước 4 (Saga Orchestration Flow):
     * Nhận event SHIPPING_BOOKING_FAILED từ Shipping Service khi đối tác 3PL Timeout/Failure sau 3 lần retry.
     * Cập nhật Order sang REVERTING_INVENTORY và ra lệnh REVERT_INVENTORY_COMMAND tới Inventory Service qua Transactional Outbox.
     */
    @KafkaListener(
            topics = "${xcare.topics.shipping-events:shipping-events}",
            groupId = "xcare-order-saga-orchestrator-group"
    )
    @Transactional
    public void onShippingBookingFailed(ConsumerRecord<String, String> record) {
        try {
            // Lọc và chỉ xử lý event SHIPPING_BOOKING_FAILED
            if (!record.value().contains("SHIPPING_BOOKING_FAILED") && !record.value().contains("failureReason")) {
                return;
            }

            ShippingBookingFailedEvent event = objectMapper.readValue(record.value(), ShippingBookingFailedEvent.class);
            log.warn("[SAGA-ORCHESTRATOR][BƯỚC 4] Nhận báo cáo lỗi 3PL từ Shipping Service cho đơn {}: {}",
                    event.getOrderNumber(), event.getFailureReason());

            Order order = orderRepository.findById(event.getOrderId()).orElse(null);
            if (order == null) {
                log.error("[SAGA-ORCHESTRATOR] Không tìm thấy đơn hàng {} để điều phối Rollback", event.getOrderId());
                return;
            }

            // Tránh xử lý trùng lặp (Idempotent Orchestration)
            if (order.getStatus() == OrderStatus.REVERTING_INVENTORY || 
                order.getStatus() == OrderStatus.ORDER_FAILED_SHIPPING_ERROR) {
                log.info("[SAGA-ORCHESTRATOR] Đơn hàng {} đã ở trạng thái {}, bỏ qua xử lý lặp",
                        order.getOrderNumber(), order.getStatus());
                return;
            }

            // 1. Chuyển trạng thái đơn hàng sang REVERTING_INVENTORY
            order.setStatus(OrderStatus.REVERTING_INVENTORY);
            order.setNotes("Saga Rollback in progress: 3PL Failure [" + event.getFailureReason() + "]");
            orderRepository.save(order);
            log.info("[SAGA-ORCHESTRATOR] Đã chuyển Order {} sang trạng thái REVERTING_INVENTORY", order.getOrderNumber());

            // 2. Chuẩn bị Command REVERT_INVENTORY_COMMAND
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

            // 3. Ghi Command vào Transactional Outbox (PostgreSQL) trong cùng Transaction
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("ORDER_SAGA")
                    .aggregateId(order.getId().toString())
                    .eventType("REVERT_INVENTORY_COMMAND")
                    .topic(TOPIC_INVENTORY_COMMANDS)
                    .partitionKey(order.getPharmacyHubId())
                    .payload(commandPayload)
                    .status(OutboxStatus.PUBLISHED)
                    .retryCount(0)
                    .createdAt(Instant.now())
                    .publishedAt(Instant.now())
                    .build();
            outboxEventRepository.save(outboxEvent);

            // 4. Phát Kafka Command tới Inventory Service
            kafkaTemplate.send(TOPIC_INVENTORY_COMMANDS, order.getPharmacyHubId(), commandPayload);
            log.info("[SAGA-ORCHESTRATOR][BƯỚC 4] Đã phát REVERT_INVENTORY_COMMAND sang topic '{}' cho kho {}",
                    TOPIC_INVENTORY_COMMANDS, order.getPharmacyHubId());

        } catch (Exception e) {
            log.error("[SAGA-ORCHESTRATOR] Lỗi xử lý event SHIPPING_BOOKING_FAILED: {}", e.getMessage(), e);
            throw new RuntimeException("Orchestration step 4 failed", e);
        }
    }

    /**
     * Bước 6 (Saga Orchestration Flow - Kết thúc):
     * Nhận event INVENTORY_RELEASED từ Inventory Service sau khi hoàn trả tồn kho Atomic SQL thành công.
     * Cập nhật Order sang ORDER_FAILED_SHIPPING_ERROR, hoàn tất chuỗi Saga Rollback!
     */
    @KafkaListener(
            topics = "${xcare.outbox.topics.inventory-events:inventory-events}",
            groupId = "xcare-order-saga-completion-group"
    )
    @Transactional
    public void onInventoryReleased(ConsumerRecord<String, String> record) {
        try {
            InventoryReleasedEvent event = objectMapper.readValue(record.value(), InventoryReleasedEvent.class);
            
            Order order = orderRepository.findById(event.getOrderId()).orElse(null);
            if (order == null) {
                return;
            }

            // Chỉ xử lý hoàn tất nếu đơn đang trong quá trình REVERTING_INVENTORY
            if (order.getStatus() == OrderStatus.REVERTING_INVENTORY) {
                order.setStatus(OrderStatus.ORDER_FAILED_SHIPPING_ERROR);
                order.setNotes("Saga Rollback Hoàn tất: 3PL Booking Timeout/Failure. Kho đã nhả tồn kho Atomic. Đơn hủy an toàn.");
                orderRepository.save(order);

                log.info("[SAGA-ORCHESTRATOR][BƯỚC 6] HOÀN TẤT SAGA ROLLBACK! Đơn hàng {} đã chuyển thành ORDER_FAILED_SHIPPING_ERROR",
                        order.getOrderNumber());
            }

        } catch (Exception e) {
            log.error("[SAGA-ORCHESTRATOR] Lỗi hoàn tất Saga kết thúc: {}", e.getMessage(), e);
        }
    }
}
