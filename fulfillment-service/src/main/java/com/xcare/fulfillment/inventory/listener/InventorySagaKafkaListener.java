package com.xcare.fulfillment.inventory.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.fulfillment.inventory.event.ShipmentCancelledEvent;
import com.xcare.fulfillment.inventory.service.InventoryReleaseService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka Consumer lắng nghe thông báo hủy vận chuyển từ Shipping Service (Bước 3 trong Saga).
 * Lắng nghe Topic: 'shipping-cancellation-events'
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventorySagaKafkaListener {

    private final InventoryReleaseService inventoryReleaseService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${xcare.topics.shipping-cancellation:shipping-cancellation-events}",
            groupId = "xcare-inventory-cancellation-group"
    )
    public void onShipmentCancelled(ConsumerRecord<String, String> record) {
        log.info("BƯỚC 3 (Inventory Kafka Listener): Nhận message từ topic [{}] key [{}]",
                record.topic(), record.key());

        try {
            ShipmentCancelledEvent event = objectMapper.readValue(record.value(), ShipmentCancelledEvent.class);
            if ("SHIPMENT_CANCELLED".equalsIgnoreCase(event.getStatus())) {
                inventoryReleaseService.releaseInventory(event);
            } else {
                log.warn("Bỏ qua event không có status SHIPMENT_CANCELLED: {}", event.getStatus());
            }
        } catch (Exception e) {
            log.error("Lỗi khi xử lý message ShipmentCancelledEvent từ Kafka: {}", record.value(), e);
        }
    }
}
