package com.xcare.fulfillment.inventory.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.fulfillment.inventory.event.RevertInventoryCommand;
import com.xcare.fulfillment.inventory.event.TransferStockCommand;
import com.xcare.fulfillment.inventory.service.InventoryReleaseService;
import com.xcare.fulfillment.inventory.service.StockTransferService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * BƯỚC 2 trong Task 4 Saga Orchestration Flow:
 * Consumer lắng nghe các Command liên quan tới Kho từ Kafka Topic 'inventory-commands'.
 *
 * BẢO VỆ CẤP HẠ TẦNG:
 * 1. MANUAL ACKNOWLEDGMENT: Tắt Auto-Commit, chỉ gọi ack.acknowledge() khi DB Transaction commit thành công.
 * 2. SPRING RETRY: @RetryableTopic thử lại 3 lần với Exponential Backoff (1000ms x 2.0).
 * 3. DEAD-LETTER QUEUE (DLQ): Khi retry quá 3 lần, tự động routing sang 'inventory-commands-DLQ'
 *    qua @DltHandler và commit offset để tránh Head-of-Line Blocking.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryCommandKafkaListener {

    private final StockTransferService stockTransferService;
    private final InventoryReleaseService inventoryReleaseService;
    private final ObjectMapper objectMapper;

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            dltTopicSuffix = "-DLQ"
    )
    @KafkaListener(
            topics = "${xcare.topics.inventory-commands:inventory-commands}",
            groupId = "xcare-inventory-commands-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onInventoryCommand(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("[INVENTORY-COMMAND-LISTENER][BƯỚC 2] Nhận Command từ topic [{}] key [{}] offset [{}]",
                record.topic(), record.key(), record.offset());

        try {
            JsonNode rootNode = objectMapper.readTree(record.value());

            if (rootNode.has("transferId") || rootNode.has("toHubId")) {
                // Nhận TransferStockCommand từ Procurement Service (:8084)
                TransferStockCommand command = objectMapper.treeToValue(rootNode, TransferStockCommand.class);
                log.info("[INVENTORY-COMMAND-LISTENER] Xử lý TransferStockCommand cho transferId [{}] từ [{}] đến [{}]",
                        command.getTransferId(), command.getFromHubId(), command.getToHubId());

                stockTransferService.processStockTransferCommand(command);

            } else if (rootNode.has("orderId") || rootNode.has("orderNumber")) {
                // Nhận RevertInventoryCommand từ Order Saga Orchestrator (:8081)
                RevertInventoryCommand command = objectMapper.treeToValue(rootNode, RevertInventoryCommand.class);
                log.info("[INVENTORY-COMMAND-LISTENER] Xử lý RevertInventoryCommand cho đơn [{}] tại Hub [{}]",
                        command.getOrderNumber(), command.getPharmacyHubId());

                inventoryReleaseService.processRevertInventoryCommand(command);
            } else {
                log.warn("[INVENTORY-COMMAND-LISTENER] Nhận message payload không xác định được schema: {}", record.value());
            }

            // MANUAL ACK: Chỉ commit offset khi và chỉ khi toàn bộ DB + Outbox đã commit thành công
            if (ack != null) {
                ack.acknowledge();
                log.debug("[INVENTORY-COMMAND-LISTENER] Đã gửi Manual ACK cho offset [{}]", record.offset());
            }

        } catch (Exception e) {
            log.error("[INVENTORY-COMMAND-LISTENER] Xử lý thất bại Command tại offset [{}]. Kích hoạt Spring Retry: {}",
                    record.offset(), e.getMessage());
            // Ném ngoại lệ để Spring @RetryableTopic kích hoạt Exponential Backoff retry
            throw new RuntimeException("Lỗi xử lý Inventory Command, yêu cầu Retry", e);
        }
    }

    /**
     * DLQ Handler tiếp nhận Poison Pill khi đã thử lại 3 lần thất bại (Exhausted).
     * Gọi ack.acknowledge() để cô lập tin nhắn độc và không gây nghẽn hàng đợi (No Head-of-Line Blocking).
     */
    @DltHandler
    public void handlePoisonPill(
            ConsumerRecord<String, String> record,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        log.error("[INVENTORY-DLQ-HANDLER][CẢNH BÁO NGUY CẤP] Tin nhắn độc (Poison Pill) đã được cách ly vào DLQ topic [{}]: offset=[{}], key=[{}], payload=[{}]",
                topic, record.offset(), record.key(), record.value());

        // Ghi nhận Audit Log / Cảnh báo SRE / Alert Notification tại đây
        // ...

        if (ack != null) {
            ack.acknowledge();
            log.info("[INVENTORY-DLQ-HANDLER] Đã Manual ACK tin nhắn độc tại topic DLQ [{}] để giải phóng hàng đợi.", topic);
        }
    }
}
