package com.xcare.fulfillment.inventory.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.fulfillment.inventory.domain.FailedInventoryCommand;
import com.xcare.fulfillment.inventory.event.RevertInventoryCommand;
import com.xcare.fulfillment.inventory.event.TransferStockCommand;
import com.xcare.fulfillment.inventory.repository.FailedInventoryCommandRepository;
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

import java.time.Instant;

/**
 * BƯỚC 2 trong Task 4 Saga Orchestration Flow:
 * Consumer lắng nghe các Command liên quan tới Kho từ Kafka Topic 'inventory-commands'.
 *
 * BẢO VỆ CẤP HẠ TẦNG:
 * 1. MANUAL ACKNOWLEDGMENT: Tắt Auto-Commit, chỉ gọi ack.acknowledge() khi DB Transaction commit thành công.
 * 2. SPRING RETRY: @RetryableTopic thử lại 3 lần với Exponential Backoff (1000ms x 2.0).
 * 3. DEAD-LETTER QUEUE (DLQ) & DB AUDIT: Khi retry quá 3 lần, tự động routing sang 'inventory-commands-DLQ'
 *    qua @DltHandler, lưu chi tiết vào bảng 'failed_inventory_commands' và commit offset trong finally block
 *    để triệt tiêu hoàn toàn nghẽn hàng chờ (No Head-of-Line Blocking).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryCommandKafkaListener {

    private final StockTransferService stockTransferService;
    private final InventoryReleaseService inventoryReleaseService;
    private final FailedInventoryCommandRepository failedInventoryCommandRepository;
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
     * Bóc tách thông tin, ghi vết vào bảng 'failed_inventory_commands' và gọi ack.acknowledge()
     * trong khối finally để cô lập tin nhắn độc và không gây nghẽn hàng đợi (No Head-of-Line Blocking).
     */
    @DltHandler
    public void handlePoisonPill(
            ConsumerRecord<String, String> record,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = KafkaHeaders.EXCEPTION_FQCN, required = false) String exceptionFqcn,
            @Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage) {

        log.error("[INVENTORY-DLQ-HANDLER][CẢNH BÁO NGUY CẤP] Tin nhắn độc (Poison Pill) đã được cách ly vào DLQ topic [{}]: partition=[{}], offset=[{}], key=[{}], payload=[{}]",
                topic, record.partition(), record.offset(), record.key(), record.value());

        try {
            // 1. Bóc tách transferId hoặc orderNumber từ payload (nếu có)
            String transferId = null;
            String orderNumber = null;

            try {
                if (record.value() != null && !record.value().isBlank()) {
                    JsonNode rootNode = objectMapper.readTree(record.value());
                    if (rootNode.has("transferId")) {
                        transferId = rootNode.get("transferId").asText();
                    }
                    if (rootNode.has("orderNumber")) {
                        orderNumber = rootNode.get("orderNumber").asText();
                    } else if (rootNode.has("orderId")) {
                        orderNumber = rootNode.get("orderId").asText();
                    }
                }
            } catch (Exception parseEx) {
                log.warn("[INVENTORY-DLQ-HANDLER] Payload không thể parse JSON: {}. Tiếp tục lưu raw text.", parseEx.getMessage());
            }

            // 2. Lưu đầy đủ thông tin tin nhắn độc vào bảng failed_inventory_commands
            FailedInventoryCommand failedCommand = FailedInventoryCommand.builder()
                    .transferId(transferId)
                    .orderNumber(orderNumber)
                    .topic(topic != null ? topic : record.topic())
                    .kafkaPartition(record.partition())
                    .kafkaOffset(record.offset())
                    .payload(record.value())
                    .exceptionClass(exceptionFqcn != null ? exceptionFqcn : "org.springframework.kafka.listener.ListenerExecutionFailedException")
                    .errorMessage(exceptionMessage != null ? exceptionMessage : "Exhausted 3 retries in @RetryableTopic")
                    .status("FAILED")
                    .retryCount(3)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();

            failedInventoryCommandRepository.save(failedCommand);
            log.info("[INVENTORY-DLQ-HANDLER] Đã lưu thành công Poison Pill vào bảng 'failed_inventory_commands' (ID=[{}], transferId=[{}], orderNumber=[{}])",
                    failedCommand.getId(), transferId, orderNumber);

        } catch (Exception dbEx) {
            log.error("[INVENTORY-DLQ-HANDLER] LỖI khi lưu Poison Pill vào database: {}", dbEx.getMessage(), dbEx);
        } finally {
            // 3. ĐẢM BẢO MANUAL ACK LUÔN LUÔN ĐƯỢC GỌI TRONG FINALLY
            // Giải phóng queue Kafka ngay cả khi việc ghi Database gặp sự cố bất ngờ
            if (ack != null) {
                ack.acknowledge();
                log.info("[INVENTORY-DLQ-HANDLER] Đã Manual ACK offset [{}] tại DLQ topic [{}] thành công trong finally block.",
                        record.offset(), topic);
            }
        }
    }
}
