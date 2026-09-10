package com.xcare.fulfillment.inventory.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.fulfillment.inventory.event.InventoryReleasedEvent;
import com.xcare.fulfillment.inventory.event.RevertInventoryCommand;
import com.xcare.fulfillment.inventory.repository.HubStockRepository;
import com.xcare.fulfillment.pack.domain.FulfillmentOutboxEvent;
import com.xcare.fulfillment.pack.repository.FulfillmentOutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Senior Implementation: Inventory Command Handler for Saga Orchestration.
 * KHÔNG DÙNG REDIS: Sử dụng Atomic SQL Queries trực tiếp trên PostgreSQL đảm bảo ACID tuyệt đối.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryCommandHandler {

    private final HubStockRepository hubStockRepository;
    private final FulfillmentOutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final String TOPIC_INVENTORY_EVENTS = "inventory-events";

    /**
     * Bước 5 (Saga Orchestration Flow):
     * Nhận REVERT_INVENTORY_COMMAND từ Orchestrator.
     * Thực thi Atomic SQL UPDATE trên PostgreSQL để hoàn trả tồn kho an toàn tuyệt đối.
     * Ghi Outbox Event và bắn INVENTORY_RELEASED về cho Orchestrator.
     */
    @KafkaListener(
            topics = "${xcare.topics.inventory-commands:inventory-commands}",
            groupId = "xcare-inventory-orchestration-command-group"
    )
    @Transactional
    public void onRevertInventoryCommand(ConsumerRecord<String, String> record) {
        try {
            RevertInventoryCommand command = objectMapper.readValue(record.value(), RevertInventoryCommand.class);
            log.info("[SAGA-STEP-5][INVENTORY] Tiếp nhận REVERT_INVENTORY_COMMAND cho đơn {} tại chi nhánh {}. Lý do: {}",
                    command.getOrderNumber(), command.getPharmacyHubId(), command.getReason());

            List<InventoryReleasedEvent.ReleasedItemPayload> releasedItems = new ArrayList<>();

            // Thực thi Atomic SQL Query cho từng SKU trực tiếp trên PostgreSQL (KHÔNG DÙNG REDIS)
            if (command.getItems() != null) {
                for (RevertInventoryCommand.RevertItemPayload item : command.getItems()) {
                    int rowsUpdated = hubStockRepository.releaseStockAtomic(
                            command.getPharmacyHubId(),
                            item.getSku(),
                            item.getQuantity()
                    );

                    if (rowsUpdated > 0) {
                        log.info("[SAGA-STEP-5][ATOMIC-SQL] Hoàn trả thành công SKU {} (+{} khả dụng) tại Hub {} qua PostgreSQL Atomic Query",
                                item.getSku(), item.getQuantity(), command.getPharmacyHubId());
                        releasedItems.add(new InventoryReleasedEvent.ReleasedItemPayload(item.getSku(), item.getQuantity()));
                    } else {
                        log.warn("[SAGA-STEP-5][ATOMIC-SQL] Không tìm thấy dòng tồn kho để hoàn trả cho SKU {} tại Hub {}",
                                item.getSku(), command.getPharmacyHubId());
                    }
                }
            }

            // Chuẩn bị Event INVENTORY_RELEASED phản hồi cho Orchestrator
            InventoryReleasedEvent event = InventoryReleasedEvent.builder()
                    .eventId(UUID.randomUUID())
                    .sagaId(command.getSagaId())
                    .orderId(command.getOrderId())
                    .orderNumber(command.getOrderNumber())
                    .pharmacyHubId(command.getPharmacyHubId())
                    .status("INVENTORY_RELEASED")
                    .cancellationReason(command.getReason())
                    .sagaType("SHIPPING_FAILURE_ROLLBACK")
                    .releasedAt(Instant.now())
                    .items(releasedItems)
                    .build();

            String eventPayload = objectMapper.writeValueAsString(event);

            // Lưu vào Transactional Outbox (PostgreSQL) trong cùng Transaction với câu lệnh UPDATE kho
            FulfillmentOutboxEvent outboxEvent = FulfillmentOutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("INVENTORY")
                    .aggregateId(command.getOrderId().toString())
                    .eventType("INVENTORY_RELEASED")
                    .topic(TOPIC_INVENTORY_EVENTS)
                    .partitionKey(command.getPharmacyHubId())
                    .payload(eventPayload)
                    .status("PUBLISHED")
                    .retryCount(0)
                    .publishedAt(Instant.now())
                    .build();
            outboxEventRepository.save(outboxEvent);

            // Bắn Kafka Event sang topic 'inventory-events' cho Orchestrator
            kafkaTemplate.send(TOPIC_INVENTORY_EVENTS, command.getOrderId().toString(), eventPayload);
            log.info("[SAGA-STEP-5-OUTBOX] Đã lưu Outbox và phát event INVENTORY_RELEASED sang Kafka topic '{}' cho đơn {}",
                    TOPIC_INVENTORY_EVENTS, command.getOrderNumber());

        } catch (Exception e) {
            log.error("[SAGA-STEP-5] Lỗi thực thi REVERT_INVENTORY_COMMAND: {}", e.getMessage(), e);
            throw new RuntimeException("Revert inventory command execution failed", e);
        }
    }
}
