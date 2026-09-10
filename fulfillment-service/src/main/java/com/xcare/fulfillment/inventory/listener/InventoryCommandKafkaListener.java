package com.xcare.fulfillment.inventory.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.fulfillment.inventory.event.RevertInventoryCommand;
import com.xcare.fulfillment.inventory.service.InventoryReleaseService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * BƯỚC 4 trong Saga Orchestration Flow:
 * Lắng nghe RevertInventoryCommand từ Order Saga Orchestrator (:8081).
 * Lắng nghe Topic: 'inventory-commands'
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryCommandKafkaListener {

    private final InventoryReleaseService inventoryReleaseService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${xcare.topics.inventory-commands:inventory-commands}",
            groupId = "xcare-inventory-commands-group"
    )
    public void onRevertInventoryCommand(ConsumerRecord<String, String> record) {
        log.info("[INVENTORY-COMMAND-LISTENER][BƯỚC 4] Nhận RevertInventoryCommand từ topic [{}] key [{}] offset [{}]",
                record.topic(), record.key(), record.offset());

        try {
            RevertInventoryCommand command = objectMapper.readValue(record.value(), RevertInventoryCommand.class);
            log.info("[INVENTORY-COMMAND-LISTENER] Bắt đầu nhả kho cho đơn [{}] - Hub [{}] - {} items",
                    command.getOrderNumber(), command.getPharmacyHubId(),
                    command.getItems() != null ? command.getItems().size() : 0);

            inventoryReleaseService.processRevertInventoryCommand(command);

        } catch (Exception e) {
            log.error("[INVENTORY-COMMAND-LISTENER] Lỗi khi xử lý RevertInventoryCommand từ Kafka: {}",
                    record.value(), e);
        }
    }
}
