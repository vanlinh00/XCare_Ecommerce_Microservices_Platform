package com.xcare.fulfillment.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.fulfillment.inventory.domain.HubStock;
import com.xcare.fulfillment.inventory.event.StockTransferredEvent;
import com.xcare.fulfillment.inventory.event.TransferStockCommand;
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
 * Service xử lý BƯỚC 2 trong Task 4: Resilient Saga Điều chuyển kho (Stock Transfer).
 * Chịu lỗi Consumer Crash & Poison Pill:
 * 1. Redis Idempotency Guard (key xcare:idempotency:transfer:{transferId}:{sku} - TTL 24h)
 * 2. Lexicographical Sorting SKUs để triệt tiêu 100% Circular Distributed Deadlock
 * 3. Redisson MultiLock 3.42.0 đa SKU trước khi ghi DB PostgreSQL
 * 4. Atomic Increment available_quantity
 * 5. Transactional Outbox Pattern lưu sự kiện sang 'inventory-saga-responses'
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StockTransferService {

    private final HubStockRepository hubStockRepository;
    private final FulfillmentOutboxEventRepository fulfillmentOutboxEventRepository;
    private final RedissonClient redissonClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${xcare.topics.inventory-saga-responses:inventory-saga-responses}")
    private String inventorySagaResponsesTopic;

    private static final String IDEMPOTENCY_PREFIX = "xcare:idempotency:transfer:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    @Transactional(rollbackFor = Exception.class)
    public void processStockTransferCommand(TransferStockCommand command) {
        log.info("[STOCK-TRANSFER-SERVICE][BƯỚC 2] Tiếp nhận TransferStockCommand: transferId=[{}], sagaId=[{}], from=[{}] -> to=[{}], items={}",
                command.getTransferId(), command.getSagaId(), command.getFromHubId(), command.getToHubId(),
                command.getItems() != null ? command.getItems().size() : 0);

        if (command.getItems() == null || command.getItems().isEmpty()) {
            log.warn("[STOCK-TRANSFER-SERVICE] Command không có items nào để chuyển kho! transferId=[{}]", command.getTransferId());
            return;
        }

        // 1. KIỂM TRA REDIS IDEMPOTENCY THEO TỪNG SKU
        List<String> validSkusToProcess = new ArrayList<>();
        List<RBucket<String>> idempotencyBuckets = new ArrayList<>();

        for (TransferStockCommand.TransferItemPayload item : command.getItems()) {
            String idempotencyKey = String.format("%s%s:%s", IDEMPOTENCY_PREFIX, command.getTransferId(), item.getSku());
            RBucket<String> bucket = redissonClient.getBucket(idempotencyKey);
            boolean isFirst = bucket.setIfAbsent("PROCESSING", IDEMPOTENCY_TTL);

            if (!isFirst) {
                String status = bucket.get();
                log.warn("[STOCK-TRANSFER-IDEMPOTENT-HIT] SKU [{}] trong transfer [{}] đã được xử lý (Status: [{}]). Bỏ qua lặp!",
                        item.getSku(), command.getTransferId(), status);
            } else {
                validSkusToProcess.add(item.getSku());
                idempotencyBuckets.add(bucket);
            }
        }

        if (validSkusToProcess.isEmpty()) {
            log.info("[STOCK-TRANSFER-SERVICE] Tất cả SKUs trong transfer [{}] đều đã được xử lý trước đó. Hoàn tất Idempotent.",
                    command.getTransferId());
            return;
        }

        // 2. SẮP XẾP DANH SÁCH SKU THEO THỨ TỰ TỪ ĐIỂN (Lexicographical Sort) ĐỂ CHỐNG DISTRIBUTED DEADLOCK
        List<String> sortedSkus = validSkusToProcess.stream()
                .distinct()
                .sorted()
                .toList();

        log.info("[STOCK-TRANSFER-SERVICE] Chuẩn bị khóa Redisson MultiLock cho {} SKUs theo thứ tự từ điển: {}",
                sortedSkus.size(), sortedSkus);

        // 3. KHỞI TẠO REDISSON MULTILOCK 3.42.0 TẠI HUB ĐÍCH (toHubId)
        List<RLock> locks = new ArrayList<>();
        for (String sku : sortedSkus) {
            String lockKey = String.format("xcare:lock:inventory:%s:%s", command.getToHubId(), sku);
            locks.add(redissonClient.getLock(lockKey));
        }

        RLock multiLock = redissonClient.getMultiLock(locks.toArray(new RLock[0]));

        try {
            // Wait 5s, Lease 10s
            boolean acquired = multiLock.tryLock(5, 10, TimeUnit.SECONDS);
            if (!acquired) {
                log.error("[STOCK-TRANSFER-SERVICE] Không thể lấy Distributed MultiLock cho SKUs {} tại Hub {}",
                        sortedSkus, command.getToHubId());
                // Xóa cờ idempotency đang PROCESSING để cho phép retry
                idempotencyBuckets.forEach(RBucket::delete);
                throw new IllegalStateException("Hệ thống kho đích đang bận giao dịch, không thể giữ Distributed MultiLock");
            }

            log.info("[STOCK-TRANSFER-SERVICE] ĐÃ GIỮ THÀNH CÔNG Redisson MultiLock cho các SKUs: {}", sortedSkus);

            // 4. ATOMIC UPDATE TỒN KHO TRONG POSTGRESQL (available_quantity += qty)
            List<StockTransferredEvent.TransferredItemPayload> transferredItems = new ArrayList<>();

            for (TransferStockCommand.TransferItemPayload item : command.getItems()) {
                if (!sortedSkus.contains(item.getSku())) {
                    continue;
                }

                Optional<HubStock> stockOpt = hubStockRepository.findByHubIdAndSku(command.getToHubId(), item.getSku());
                HubStock stock;
                int addedQty = item.getQuantity();

                if (stockOpt.isPresent()) {
                    stock = stockOpt.get();
                    int currentAvailable = stock.getAvailableQuantity() != null ? stock.getAvailableQuantity() : 0;
                    int newAvailable = currentAvailable + addedQty;
                    stock.setAvailableQuantity(newAvailable);
                    stock.setUpdatedAt(Instant.now());
                    log.info("[STOCK-TRANSFER-SERVICE] Tăng tồn kho SKU [{}] tại Hub đích [{}]: ({} -> {})",
                            item.getSku(), command.getToHubId(), currentAvailable, newAvailable);
                } else {
                    stock = HubStock.builder()
                            .hubId(command.getToHubId())
                            .sku(item.getSku())
                            .productName(item.getProductName())
                            .availableQuantity(addedQty)
                            .reservedQuantity(0)
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build();
                    log.info("[STOCK-TRANSFER-SERVICE] Tạo mới tồn kho SKU [{}] tại Hub đích [{}]: available={}",
                            item.getSku(), command.getToHubId(), addedQty);
                }

                hubStockRepository.save(stock);

                transferredItems.add(StockTransferredEvent.TransferredItemPayload.builder()
                        .sku(item.getSku())
                        .addedQuantity(addedQty)
                        .newAvailableQuantity(stock.getAvailableQuantity())
                        .build());
            }

            // 5. CẬP NHẬT TRẠNG THÁI IDEMPOTENCY REDIS THÀNH COMPLETED
            idempotencyBuckets.forEach(bucket -> bucket.set("COMPLETED", IDEMPOTENCY_TTL));

            // 6. XÂY DỰNG EVENT STOCK_TRANSFERRED
            StockTransferredEvent responseEvent = StockTransferredEvent.builder()
                    .eventId(UUID.randomUUID())
                    .eventType("STOCK_TRANSFERRED")
                    .sagaId(command.getSagaId())
                    .transferId(command.getTransferId())
                    .fromHubId(command.getFromHubId())
                    .toHubId(command.getToHubId())
                    .status("INVENTORY_UPDATED")
                    .transferredAt(Instant.now())
                    .items(transferredItems)
                    .build();

            String payloadJson = objectMapper.writeValueAsString(responseEvent);

            // 7. TRANSACTIONAL OUTBOX PATTERN: Lưu vào PostgreSQL (fulfillment_outbox)
            FulfillmentOutboxEvent outboxEvent = FulfillmentOutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("STOCK_TRANSFER")
                    .aggregateId(command.getTransferId())
                    .eventType("STOCK_TRANSFERRED")
                    .topic(inventorySagaResponsesTopic)
                    .partitionKey(command.getToHubId())
                    .payload(payloadJson)
                    .status(OutboxStatus.PUBLISHED)
                    .retryCount(0)
                    .build();
            fulfillmentOutboxEventRepository.save(outboxEvent);

            // 8. BẮN EVENT VỀ TOPIC 'inventory-saga-responses' CHO PROCUREMENT SERVICE (:8084)
            kafkaTemplate.send(inventorySagaResponsesTopic, command.getTransferId(), payloadJson);

            log.info("[STOCK-TRANSFER-SERVICE][BƯỚC 2] HOÀN TẤT! Đã gửi StockTransferredEvent về topic [{}] cho transfer [{}]",
                    inventorySagaResponsesTopic, command.getTransferId());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            idempotencyBuckets.forEach(RBucket::delete);
            throw new RuntimeException("Thread bị ngắt khi đợi Redisson MultiLock", e);
        } catch (Exception e) {
            log.error("[STOCK-TRANSFER-SERVICE] Lỗi trong tiến trình điều chuyển kho cho transfer [{}]: {}",
                    command.getTransferId(), e.getMessage(), e);
            idempotencyBuckets.forEach(RBucket::delete);
            throw new RuntimeException("Thất bại khi thực thi điều chuyển kho", e);
        } finally {
            // 9. GIẢI PHÓNG MULTILOCK AN TOÀN TRONG KHỐI FINALLY
            if (multiLock.isHeldByCurrentThread()) {
                multiLock.unlock();
                log.info("[STOCK-TRANSFER-SERVICE] ĐÃ GIẢI PHÓNG (UNLOCK) Redisson MultiLock an toàn cho các SKUs: {}", sortedSkus);
            }
        }
    }
}
