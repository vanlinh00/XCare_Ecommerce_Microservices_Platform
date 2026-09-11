package com.xcare.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.order.domain.entity.OutboxEvent;
import com.xcare.order.domain.enums.OutboxStatus;
import com.xcare.order.event.OnlineStockSyncedEvent;
import com.xcare.order.event.SyncOnlineStockCommand;
import com.xcare.order.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Service xử lý BƯỚC 4 trong Task 4:
 * Đồng bộ trạng thái tồn kho lên Catalog Online và mở bán lại SKU cho khách hàng mua sắm.
 * Tích hợp Redis Idempotency Guard (key xcare:idempotency:catalog:sync:{transferId}:{sku})
 * và Transactional Outbox Pattern bắn OnlineStockSyncedEvent về Topic 'inventory-saga-responses'.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CatalogStockSyncService {

    private final OutboxEventRepository outboxEventRepository;
    private final RedissonClient redissonClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${xcare.outbox.topics.inventory-saga-responses:inventory-saga-responses}")
    private String inventorySagaResponsesTopic;

    private static final String IDEMPOTENCY_PREFIX = "xcare:idempotency:catalog:sync:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    @Transactional(rollbackFor = Exception.class)
    public void processSyncOnlineStockCommand(SyncOnlineStockCommand command) {
        log.info("[CATALOG-SYNC-SERVICE][BƯỚC 4] Nhận SyncOnlineStockCommand: transferId=[{}], targetHub=[{}], items={}",
                command.getTransferId(), command.getTargetHubId(),
                command.getItems() != null ? command.getItems().size() : 0);

        if (command.getItems() == null || command.getItems().isEmpty()) {
            log.warn("[CATALOG-SYNC-SERVICE] Không có SKU nào cần đồng bộ catalog cho transfer [{}]", command.getTransferId());
            return;
        }

        List<OnlineStockSyncedEvent.SyncedCatalogItemPayload> syncedItems = new ArrayList<>();

        for (SyncOnlineStockCommand.CatalogSyncItemPayload item : command.getItems()) {
            String idempotencyKey = String.format("%s%s:%s", IDEMPOTENCY_PREFIX, command.getTransferId(), item.getSku());
            RBucket<String> bucket = redissonClient.getBucket(idempotencyKey);
            boolean isFirst = bucket.setIfAbsent("PROCESSING", IDEMPOTENCY_TTL);

            if (!isFirst) {
                String existingStatus = bucket.get();
                log.warn("[CATALOG-SYNC-IDEMPOTENT-HIT] SKU [{}] trong transfer [{}] đã được đồng bộ Catalog trước đó (Status: [{}]). Bỏ qua lặp!",
                        item.getSku(), command.getTransferId(), existingStatus);
                continue;
            }

            // Giả lập mở bán và cập nhật trạng thái Catalog Online cho SKU
            log.info("[CATALOG-SYNC-SERVICE] Đã mở bán SKU [{}] tại Hub [{}], số lượng bổ sung: {}, tổng khả dụng: {}",
                    item.getSku(), command.getTargetHubId(), item.getAddedQuantity(), item.getTotalAvailableQuantity());

            bucket.set("COMPLETED", IDEMPOTENCY_TTL);

            syncedItems.add(OnlineStockSyncedEvent.SyncedCatalogItemPayload.builder()
                    .sku(item.getSku())
                    .onlineAvailable(true)
                    .syncedQuantity(item.getAddedQuantity())
                    .build());
        }

        try {
            // Xây dựng Response Event báo về cho Procurement Service (:8084)
            OnlineStockSyncedEvent responseEvent = OnlineStockSyncedEvent.builder()
                    .eventId(UUID.randomUUID())
                    .eventType("ONLINE_STOCK_SYNCED")
                    .sagaId(command.getSagaId())
                    .transferId(command.getTransferId())
                    .targetHubId(command.getTargetHubId())
                    .status("COMPLETED")
                    .syncedAt(Instant.now())
                    .items(syncedItems)
                    .build();

            String payloadJson = objectMapper.writeValueAsString(responseEvent);

            // Ghi Transactional Outbox vào PostgreSQL (orders_db)
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("CATALOG_SYNC")
                    .aggregateId(command.getTransferId())
                    .eventType("ONLINE_STOCK_SYNCED")
                    .topic(inventorySagaResponsesTopic)
                    .partitionKey(command.getTargetHubId() != null ? command.getTargetHubId() : command.getTransferId())
                    .payload(payloadJson)
                    .status(OutboxStatus.PUBLISHED)
                    .retryCount(0)
                    .createdAt(Instant.now())
                    .publishedAt(Instant.now())
                    .build();

            outboxEventRepository.save(outboxEvent);

            // Gửi message sang Kafka topic 'inventory-saga-responses'
            kafkaTemplate.send(inventorySagaResponsesTopic, command.getTransferId(), payloadJson);

            log.info("[CATALOG-SYNC-SERVICE][BƯỚC 4] Đồng bộ Catalog Online thành công! Đã gửi OnlineStockSyncedEvent sang topic [{}]",
                    inventorySagaResponsesTopic);

        } catch (Exception e) {
            log.error("[CATALOG-SYNC-SERVICE] Lỗi khi tạo Outbox hoặc gửi event đồng bộ catalog: {}", e.getMessage(), e);
            throw new RuntimeException("Lỗi xử lý đồng bộ Catalog Online", e);
        }
    }
}
