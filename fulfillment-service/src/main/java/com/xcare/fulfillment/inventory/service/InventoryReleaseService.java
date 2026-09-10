package com.xcare.fulfillment.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.fulfillment.inventory.domain.HubStock;
import com.xcare.fulfillment.inventory.event.InventoryReleasedEvent;
import com.xcare.fulfillment.inventory.event.RevertInventoryCommand;
import com.xcare.fulfillment.inventory.event.ShipmentCancelledEvent;
import com.xcare.fulfillment.inventory.repository.HubStockRepository;
import com.xcare.fulfillment.pack.domain.FulfillmentOutboxEvent;
import com.xcare.fulfillment.pack.domain.OutboxStatus;
import com.xcare.fulfillment.pack.repository.FulfillmentOutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * BƯỚC 4 trong Saga Orchestration Flow: Hoàn trả tồn kho (Compensating Transaction).
 * - Kiểm tra Idempotent Consumer bằng Redis 7.2 (setIfAbsent với TTL 24h).
 * - Sắp xếp SKU theo thứ tự từ điển (Lexicographical Sort) để chống Distributed Deadlock.
 * - Khóa phân tán Redisson MultiLock 3.42.0 đa SKU trước khi cập nhật số lượng tồn kho.
 * - Transactional Outbox Pattern lưu vào PostgreSQL (fulfillment_outbox).
 * - Bắn Response Event INVENTORY_RELEASED về topic 'order-saga-responses' cho Orchestrator.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryReleaseService {

    private final HubStockRepository hubStockRepository;
    private final FulfillmentOutboxEventRepository fulfillmentOutboxEventRepository;
    private final RedissonClient redissonClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${xcare.topics.order-saga-responses:order-saga-responses}")
    private String orderSagaResponsesTopic;

    @Value("${xcare.topics.inventory-events:inventory-events}")
    private String inventoryEventsTopic;

    private static final String IDEMPOTENCY_PREFIX = "xcare:idempotency:inventory:release:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    /**
     * BƯỚC 4 (Task 1 Saga Orchestration Flow):
     * Nhận RevertInventoryCommand từ Order Saga Orchestrator (:8081).
     */
    @Transactional(rollbackFor = Exception.class)
    public void processRevertInventoryCommand(RevertInventoryCommand command) {
        String idempotencyKey = IDEMPOTENCY_PREFIX + command.getOrderId();
        log.info("[INVENTORY-SERVICE][BƯỚC 4] Tiếp nhận hoàn trả tồn kho cho đơn [{}], Hub [{}], SagaId [{}]",
                command.getOrderNumber(), command.getPharmacyHubId(), command.getSagaId());

        // 1. Kiểm tra Idempotency bằng Redis 7.2 (setIfAbsent với TTL 24h)
        RBucket<String> idempotencyBucket = redissonClient.getBucket(idempotencyKey);
        boolean isFirstExecution = idempotencyBucket.setIfAbsent("PROCESSING", IDEMPOTENCY_TTL);

        if (!isFirstExecution) {
            String currentStatus = idempotencyBucket.get();
            log.warn("[INVENTORY-SERVICE][IDEMPOTENT HIT] Kho Hub [{}] đã xử lý hoàn trả cho đơn [{}] trước đó (Trạng thái: [{}]). Bỏ qua lặp.",
                    command.getPharmacyHubId(), command.getOrderNumber(), currentStatus);
            return;
        }

        // 2. SẮP XẾP DANH SÁCH SKU THEO THỨ TỰ TỪ ĐIỂN (Lexicographical Sort) ĐỂ CHỐNG DISTRIBUTED DEADLOCK
        List<String> sortedSkus = command.getItems().stream()
                .map(RevertInventoryCommand.RevertItemPayload::getSku)
                .distinct()
                .sorted()
                .toList();

        log.info("[INVENTORY-SERVICE] Chuẩn bị khóa Redisson MultiLock cho {} SKUs theo thứ tự từ điển: {}",
                sortedSkus.size(), sortedSkus);

        // 3. Khởi tạo danh sách RLock từ Redisson và tạo Redisson MultiLock 3.42.0
        List<RLock> locks = new ArrayList<>();
        for (String sku : sortedSkus) {
            String lockKey = String.format("xcare:lock:inventory:%s:%s", command.getPharmacyHubId(), sku);
            locks.add(redissonClient.getLock(lockKey));
        }

        RLock multiLock = redissonClient.getMultiLock(locks.toArray(new RLock[0]));

        try {
            // 4. Acquire Redisson MultiLock với waitTime = 5s, leaseTime = 10s
            boolean acquired = multiLock.tryLock(5, 10, TimeUnit.SECONDS);
            if (!acquired) {
                log.error("[INVENTORY-SERVICE] Không thể lấy Distributed MultiLock cho SKUs {} tại Hub {}",
                        sortedSkus, command.getPharmacyHubId());
                idempotencyBucket.delete();
                throw new IllegalStateException("Hệ thống kho đang bận giao dịch, không thể giữ Distributed MultiLock");
            }

            log.info("[INVENTORY-SERVICE] ĐÃ GIỮ THÀNH CÔNG Redisson MultiLock cho các SKUs: {}", sortedSkus);

            // 5. Cập nhật hoàn tác kho trong PostgreSQL: (reserved_quantity -= qty, available_quantity += qty)
            List<InventoryReleasedEvent.ReleasedItemPayload> releasedItems = new ArrayList<>();

            for (RevertInventoryCommand.RevertItemPayload item : command.getItems()) {
                Optional<HubStock> stockOpt = hubStockRepository.findByHubIdAndSku(command.getPharmacyHubId(), item.getSku());

                HubStock stock;
                if (stockOpt.isPresent()) {
                    stock = stockOpt.get();
                    int qty = item.getQuantity();
                    int oldReserved = stock.getReservedQuantity() != null ? stock.getReservedQuantity() : 0;
                    int oldAvailable = stock.getAvailableQuantity() != null ? stock.getAvailableQuantity() : 0;

                    int newReserved = Math.max(0, oldReserved - qty);
                    int newAvailable = oldAvailable + qty;

                    stock.setReservedQuantity(newReserved);
                    stock.setAvailableQuantity(newAvailable);

                    log.info("[INVENTORY-SERVICE] Cập nhật tồn kho SKU [{}] tại Hub [{}]: Reserved ({} -> {}), Available ({} -> {})",
                            item.getSku(), command.getPharmacyHubId(), oldReserved, newReserved, oldAvailable, newAvailable);
                } else {
                    stock = HubStock.builder()
                            .hubId(command.getPharmacyHubId())
                            .sku(item.getSku())
                            .productName(item.getProductName())
                            .availableQuantity(item.getQuantity())
                            .reservedQuantity(0)
                            .build();
                }

                hubStockRepository.save(stock);

                releasedItems.add(InventoryReleasedEvent.ReleasedItemPayload.builder()
                        .sku(item.getSku())
                        .releasedQuantity(item.getQuantity())
                        .build());
            }

            // 6. Cập nhật trạng thái Idempotency trong Redis thành COMPLETED
            idempotencyBucket.set("COMPLETED", IDEMPOTENCY_TTL);

            // 7. Xây dựng Response Event INVENTORY_RELEASED
            InventoryReleasedEvent releasedEvent = InventoryReleasedEvent.builder()
                    .eventId(UUID.randomUUID())
                    .sagaId(command.getSagaId())
                    .orderId(command.getOrderId())
                    .orderNumber(command.getOrderNumber())
                    .pharmacyHubId(command.getPharmacyHubId())
                    .status("INVENTORY_RELEASED")
                    .cancellationReason(command.getReason())
                    .sagaType("CUSTOMER_CANCEL")
                    .releasedAt(Instant.now())
                    .items(releasedItems)
                    .build();

            String payloadJson = objectMapper.writeValueAsString(releasedEvent);

            // 8. Transactional Outbox Pattern: Lưu Outbox Event vào PostgreSQL (fulfillment_outbox) trong cùng Transaction
            FulfillmentOutboxEvent outboxEvent = FulfillmentOutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("INVENTORY")
                    .aggregateId(command.getOrderId().toString())
                    .eventType("INVENTORY_RELEASED")
                    .topic(orderSagaResponsesTopic)
                    .partitionKey(command.getPharmacyHubId())
                    .payload(payloadJson)
                    .status(OutboxStatus.PUBLISHED)
                    .retryCount(0)
                    .build();
            fulfillmentOutboxEventRepository.save(outboxEvent);

            // 9. Bắn Response Event về topic 'order-saga-responses' cho Order Saga Orchestrator
            kafkaTemplate.send(orderSagaResponsesTopic, command.getOrderNumber(), payloadJson);
            if (!orderSagaResponsesTopic.equals(inventoryEventsTopic)) {
                kafkaTemplate.send(inventoryEventsTopic, command.getOrderNumber(), payloadJson);
            }

            log.info("[INVENTORY-SERVICE][BƯỚC 4] HOÀN TẤT! Đã gửi INVENTORY_RELEASED về topic '{}' cho đơn [{}]",
                    orderSagaResponsesTopic, command.getOrderNumber());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            idempotencyBucket.delete();
            throw new RuntimeException("Thread bị ngắt khi chờ Redisson MultiLock", e);
        } catch (Exception e) {
            log.error("[INVENTORY-SERVICE] Lỗi khi giải phóng tồn kho cho đơn [{}]: {}",
                    command.getOrderNumber(), e.getMessage(), e);
            idempotencyBucket.delete();
            throw new RuntimeException("Thất bại khi thực thi hoàn tác kho", e);
        } finally {
            // 10. Giải phóng Redisson Distributed MultiLock an toàn
            if (multiLock.isHeldByCurrentThread()) {
                multiLock.unlock();
                log.info("[INVENTORY-SERVICE] ĐÃ GIẢI PHÓNG (UNLOCK) Redisson MultiLock an toàn cho các SKUs: {}", sortedSkus);
            }
        }
    }

    /**
     * Tương thích ngược với luồng Choreography Saga
     */
    @Transactional
    public void releaseInventory(ShipmentCancelledEvent event) {
        RevertInventoryCommand command = RevertInventoryCommand.builder()
                .commandId(UUID.randomUUID())
                .sagaId(event.getSagaId())
                .orderId(event.getOrderId())
                .orderNumber(event.getOrderNumber())
                .pharmacyHubId(event.getPharmacyHubId())
                .reason(event.getCancelReason())
                .createdAt(Instant.now())
                .items(event.getItems() != null ? event.getItems().stream()
                        .map(i -> RevertInventoryCommand.RevertItemPayload.builder()
                                .sku(i.getSku())
                                .productName(i.getProductName())
                                .quantity(i.getQuantity())
                                .build())
                        .toList() : java.util.Collections.emptyList())
                .build();
        processRevertInventoryCommand(command);
    }
}
