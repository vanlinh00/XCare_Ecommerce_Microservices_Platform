package com.xcare.order.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.order.event.InventoryReleasedEvent;
import com.xcare.order.event.ShipmentCancelledEvent;
import com.xcare.order.saga.OrderSagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Saga Orchestrator Response Listener (Trung tâm tiếp nhận kết quả từ các Service thành viên).
 * Lắng nghe topic 'order-saga-responses' theo mô hình Command-Response Star Topology.
 * 
 * - Nhận ShipmentCancelledEvent (Bước 2 của Shipping Service) -> Kích hoạt Bước 3 (Phát lệnh Revert Inventory).
 * - Nhận InventoryReleasedEvent (Bước 4 của Inventory Service) -> Kích hoạt Bước 5 (Hoàn tất hủy đơn & Audit Log).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrchestratorResponseKafkaListener {

    private final OrderSagaOrchestrator orderSagaOrchestrator;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = {
                    "${xcare.outbox.topics.order-saga-responses:order-saga-responses}",
                    "${xcare.outbox.topics.shipping-cancellation:shipping-cancellation-events}",
                    "${xcare.outbox.topics.inventory-events:inventory-events}"
            },
            groupId = "xcare-orchestrator-response-group"
    )
    public void onSagaResponseReceived(ConsumerRecord<String, String> record) {
        String payload = record.value();
        log.info("[ORCHESTRATOR-RESPONSE-LISTENER] Nhận message từ Topic [{}] Partition [{}] Offset [{}]",
                record.topic(), record.partition(), record.offset());

        try {
            JsonNode rootNode = objectMapper.readTree(payload);

            // 1. Phân loại Event: SHIPMENT_CANCELLED (từ Shipping Service :8082)
            if (payload.contains("SHIPMENT_CANCELLED") || rootNode.has("trackingCode") || rootNode.has("carrier")) {
                ShipmentCancelledEvent event = objectMapper.readValue(payload, ShipmentCancelledEvent.class);
                log.info("[ORCHESTRATOR-RESPONSE-LISTENER] Nhận SHIPMENT_CANCELLED cho đơn [{}] - Saga [{}]",
                        event.getOrderNumber(), event.getSagaId());
                orderSagaOrchestrator.handleShipmentCancelled(event);
                return;
            }

            // 2. Phân loại Event: INVENTORY_RELEASED (từ Inventory Service :8083)
            if (payload.contains("INVENTORY_RELEASED") || rootNode.has("releasedAt") || payload.contains("releasedQuantity")) {
                InventoryReleasedEvent event = objectMapper.readValue(payload, InventoryReleasedEvent.class);
                log.info("[ORCHESTRATOR-RESPONSE-LISTENER] Nhận INVENTORY_RELEASED cho đơn [{}] - Saga [{}]",
                        event.getOrderNumber(), event.getSagaId());
                orderSagaOrchestrator.handleInventoryReleased(event);
                return;
            }

            log.warn("[ORCHESTRATOR-RESPONSE-LISTENER] Bỏ qua payload không xác định được schema: {}", payload);

        } catch (Exception e) {
            log.error("[ORCHESTRATOR-RESPONSE-LISTENER] Lỗi xử lý saga response từ topic {}: {}",
                    record.topic(), e.getMessage(), e);
        }
    }
}
