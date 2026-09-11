package com.xcare.order.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.order.event.SyncOnlineStockCommand;
import com.xcare.order.service.CatalogStockSyncService;
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
 * BƯỚC 4 trong Task 4 Saga Orchestration Flow:
 * Consumer lắng nghe SyncOnlineStockCommand từ topic 'catalog-commands' để mở bán lại hàng Online.
 *
 * BẢO VỆ CẤP HẠ TẦNG:
 * 1. MANUAL ACKNOWLEDGMENT: Tắt Auto-Commit, chỉ gọi ack.acknowledge() sau khi cập nhật catalog và outbox commit.
 * 2. SPRING RETRY: @RetryableTopic thử lại 3 lần với Exponential Backoff (1000ms x 2.0).
 * 3. DEAD-LETTER QUEUE (DLQ): Khi retry quá 3 lần (Exhausted), tự động routing sang 'catalog-commands-DLQ'
 *    qua @DltHandler và commit offset để tránh Head-of-Line Blocking.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CatalogCommandKafkaListener {

    private final CatalogStockSyncService catalogStockSyncService;
    private final ObjectMapper objectMapper;

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            dltTopicSuffix = "-DLQ"
    )
    @KafkaListener(
            topics = "${xcare.topics.catalog-commands:catalog-commands}",
            groupId = "xcare-catalog-commands-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onCatalogCommand(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("[CATALOG-COMMAND-LISTENER][BƯỚC 4] Nhận SyncOnlineStockCommand từ topic [{}] key [{}] offset [{}]",
                record.topic(), record.key(), record.offset());

        try {
            SyncOnlineStockCommand command = objectMapper.readValue(record.value(), SyncOnlineStockCommand.class);
            log.info("[CATALOG-COMMAND-LISTENER] Bắt đầu đồng bộ Online Catalog cho transfer [{}] tại Hub [{}] - {} items",
                    command.getTransferId(), command.getTargetHubId(),
                    command.getItems() != null ? command.getItems().size() : 0);

            catalogStockSyncService.processSyncOnlineStockCommand(command);

            // MANUAL ACK: Chỉ commit offset khi toàn bộ tiến trình cập nhật Catalog thành công
            if (ack != null) {
                ack.acknowledge();
                log.debug("[CATALOG-COMMAND-LISTENER] Đã Manual ACK offset [{}] thành công", record.offset());
            }

        } catch (Exception e) {
            log.error("[CATALOG-COMMAND-LISTENER] Lỗi khi xử lý SyncOnlineStockCommand tại offset [{}]. Kích hoạt Spring Retry: {}",
                    record.offset(), e.getMessage());
            // Ném ngoại lệ để Spring @RetryableTopic kích hoạt Exponential Backoff retry
            throw new RuntimeException("Lỗi xử lý Catalog Command, yêu cầu Retry", e);
        }
    }

    /**
     * DLQ Handler tiếp nhận Poison Pill khi thử lại 3 lần thất bại (Exhausted).
     * Gọi ack.acknowledge() để cô lập tin nhắn độc và không làm nghẽn hàng đợi (No Head-of-Line Blocking).
     */
    @DltHandler
    public void handleCatalogPoisonPill(
            ConsumerRecord<String, String> record,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        log.error("[CATALOG-DLQ-HANDLER][CẢNH BÁO NGUY CẤP] Tin nhắn độc (Poison Pill) đã được cách ly vào DLQ topic [{}]: offset=[{}], key=[{}], payload=[{}]",
                topic, record.offset(), record.key(), record.value());

        // Ghi nhận Audit Log / Cảnh báo Alert tại đây
        if (ack != null) {
            ack.acknowledge();
            log.info("[CATALOG-DLQ-HANDLER] Đã Manual ACK tin nhắn độc tại topic DLQ [{}] để giải phóng hàng đợi.", topic);
        }
    }
}
