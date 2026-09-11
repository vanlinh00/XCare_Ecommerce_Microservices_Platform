package com.xcare.procurement.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.procurement.event.OnlineStockSyncedEvent;
import com.xcare.procurement.event.StockTransferredEvent;
import com.xcare.procurement.saga.StockTransferSagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumer tại Procurement Service (:8084) lắng nghe Response Events từ Topic 'inventory-saga-responses'.
 * Bật Manual ACK để đảm bảo tính an toàn giao dịch.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ProcurementResponseKafkaListener {

    private final StockTransferSagaOrchestrator sagaOrchestrator;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${xcare.topics.inventory-saga-responses:inventory-saga-responses}",
            groupId = "xcare-procurement-saga-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onSagaResponse(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("[PROCUREMENT-RESPONSE-LISTENER] Nhận message từ topic [{}] key [{}] offset [{}]",
                record.topic(), record.key(), record.offset());

        try {
            JsonNode rootNode = objectMapper.readTree(record.value());
            String eventType = rootNode.has("eventType") ? rootNode.get("eventType").asText() : "";

            if ("STOCK_TRANSFERRED".equalsIgnoreCase(eventType)) {
                // BƯỚC 3: Phản hồi từ Fulfillment Service (:8083)
                StockTransferredEvent event = objectMapper.treeToValue(rootNode, StockTransferredEvent.class);
                log.info("[PROCUREMENT-RESPONSE-LISTENER] Nhận STOCK_TRANSFERRED cho transfer [{}]", event.getTransferId());
                sagaOrchestrator.handleStockTransferred(event);

            } else if ("ONLINE_STOCK_SYNCED".equalsIgnoreCase(eventType)) {
                // BƯỚC 4: Phản hồi từ Order/Catalog Service (:8081)
                OnlineStockSyncedEvent event = objectMapper.treeToValue(rootNode, OnlineStockSyncedEvent.class);
                log.info("[PROCUREMENT-RESPONSE-LISTENER] Nhận ONLINE_STOCK_SYNCED cho transfer [{}]", event.getTransferId());
                sagaOrchestrator.handleOnlineStockSynced(event);

            } else {
                log.debug("[PROCUREMENT-RESPONSE-LISTENER] Bỏ qua eventType không xử lý: [{}]", eventType);
            }

            if (ack != null) {
                ack.acknowledge();
            }

        } catch (Exception e) {
            log.error("[PROCUREMENT-RESPONSE-LISTENER] Lỗi khi xử lý response event: {}", e.getMessage(), e);
        }
    }
}
