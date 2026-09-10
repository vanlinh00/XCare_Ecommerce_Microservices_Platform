package com.xcare.order.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.order.event.InventoryReleasedEvent;
import com.xcare.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka Consumer lắng nghe kết quả hoàn kho (Bước 4 trong Saga Choreography).
 * Lắng nghe Topic 'inventory-events'.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaKafkaListener {

    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${xcare.outbox.topics.inventory-events:inventory-events}",
            groupId = "xcare-order-saga-group"
    )
    public void onInventoryReleased(ConsumerRecord<String, String> record) {
        log.info("BƯỚC 4 (Order Service Kafka Listener): Nhận message từ topic [{}] key [{}]",
                record.topic(), record.key());

        try {
            InventoryReleasedEvent event = objectMapper.readValue(record.value(), InventoryReleasedEvent.class);

            if ("INVENTORY_RELEASED".equalsIgnoreCase(event.getStatus())) {
                log.info("Xử lý hoàn tất Saga cho đơn hàng [{}] - SagaId [{}]",
                        event.getOrderNumber(), event.getSagaId());
                orderService.completeOrderCancellation(event);
            } else {
                log.warn("Bỏ qua event không khớp status: {}", event.getStatus());
            }

        } catch (Exception e) {
            log.error("Lỗi khi xử lý message InventoryReleasedEvent từ Kafka: {}", record.value(), e);
            // Có thể đẩy vào Dead Letter Queue (DLQ) hoặc retry
        }
    }
}
