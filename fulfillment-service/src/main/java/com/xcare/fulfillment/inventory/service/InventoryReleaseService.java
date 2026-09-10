package com.xcare.fulfillment.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.fulfillment.inventory.domain.HubStock;
import com.xcare.fulfillment.inventory.event.InventoryReleasedEvent;
import com.xcare.fulfillment.inventory.event.ShipmentCancelledEvent;
import com.xcare.fulfillment.inventory.repository.HubStockRepository;
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
 * Xử lý bước 3 trong Saga Rollback: Hoàn trả tồn kho (Compensating Transaction).
 * Sử dụng Redisson Distributed Multi-Lock để đồng bộ và chống Race Condition / Deadlock.
 * Đảm bảo tính Idempotency bằng Redis Key trước khi giải phóng kho.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryReleaseService {

    private final HubStockRepository hubStockRepository;
    private final RedissonClient redissonClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${xcare.topics.inventory-events:inventory-events}")
    private String inventoryEventsTopic;

    private static final String IDEMPOTENCY_PREFIX = "xcare:idempotency:inventory:release:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    @Transactional
    public void releaseInventory(ShipmentCancelledEvent event) {
        String idempotencyKey = IDEMPOTENCY_PREFIX + event.getOrderId();
        log.info("BƯỚC 3 (Inventory Service): Tiếp nhận hoàn trả tồn kho cho đơn [{}], Hub [{}], SagaId [{}]",
                event.getOrderNumber(), event.getPharmacyHubId(), event.getSagaId());

        // 1. Kiểm tra tính Idempotency bằng Redis Key
        RBucket<String> idempotencyBucket = redissonClient.getBucket(idempotencyKey);
        boolean isFirstExecution = idempotencyBucket.setIfAbsent("PROCESSING", IDEMPOTENCY_TTL);

        if (!isFirstExecution) {
            String currentStatus = idempotencyBucket.get();
            log.warn("IDEMPOTENT HIT: Kho đã xử lý hoàn trả cho đơn [{}] từ trước (Trạng thái: [{}]). Bỏ qua lặp.",
                    event.getOrderNumber(), currentStatus);
            return;
        }

        // 2. Sắp xếp danh sách SKU theo thứ tự từ điển (Lexicographical order) để chống Deadlock phân tán
        List<String> sortedSkus = event.getItems().stream()
                .map(ShipmentCancelledEvent.ShippingItemPayload::getSku)
                .distinct()
                .sorted()
                .toList();

        List<RLock> locks = new ArrayList<>();
        for (String sku : sortedSkus) {
            String lockKey = String.format("xcare:lock:inventory:%s:%s", event.getPharmacyHubId(), sku);
            locks.add(redissonClient.getLock(lockKey));
        }

        RLock multiLock = redissonClient.getMultiLock(locks.toArray(new RLock[0]));

        try {
            // 3. Acquire Redisson Distributed MultiLock
            boolean acquired = multiLock.tryLock(5, 10, TimeUnit.SECONDS);
            if (!acquired) {
                log.error("Không thể lấy Distributed Lock cho SKUs {} tại hub {}", sortedSkus, event.getPharmacyHubId());
                idempotencyBucket.delete();
                throw new IllegalStateException("Hệ thống kho đang quá tải, không thể lấy Distributed Lock");
            }

            log.info("Đã giữ Distributed Lock cho {} SKUs tại hub {}", sortedSkus.size(), event.getPharmacyHubId());

            // 4. Hoàn trả số lượng tồn kho (Compensating action: Reserved -> Available)
            List<InventoryReleasedEvent.ReleasedItemPayload> releasedItems = new ArrayList<>();

            for (ShipmentCancelledEvent.ShippingItemPayload item : event.getItems()) {
                Optional<HubStock> stockOpt = hubStockRepository.findByHubIdAndSku(event.getPharmacyHubId(), item.getSku());

                HubStock stock;
                if (stockOpt.isPresent()) {
                    stock = stockOpt.get();
                    int qty = item.getQuantity();
                    int newReserved = Math.max(0, stock.getReservedQuantity() - qty);
                    int newAvailable = stock.getAvailableQuantity() + qty;

                    log.info("Hoàn kho SKU [{}] tại Hub [{}]: Reserved ({} -> {}), Available ({} -> {})",
                            item.getSku(), event.getPharmacyHubId(),
                            stock.getReservedQuantity(), newReserved,
                            stock.getAvailableQuantity(), newAvailable);

                    stock.setReservedQuantity(newReserved);
                    stock.setAvailableQuantity(newAvailable);
                } else {
                    // Nếu chưa có bản ghi tồn kho, tạo mới với số lượng hoàn trả
                    stock = HubStock.builder()
                            .hubId(event.getPharmacyHubId())
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

            // 5. Cập nhật trạng thái Idempotency trong Redis thành COMPLETED
            idempotencyBucket.set("COMPLETED", IDEMPOTENCY_TTL);

            // 6. Bắn Kafka Event INVENTORY_RELEASED sang topic 'inventory-events'
            InventoryReleasedEvent inventoryReleasedEvent = InventoryReleasedEvent.builder()
                    .eventId(UUID.randomUUID())
                    .sagaId(event.getSagaId())
                    .orderId(event.getOrderId())
                    .orderNumber(event.getOrderNumber())
                    .pharmacyHubId(event.getPharmacyHubId())
                    .status("INVENTORY_RELEASED")
                    .releasedAt(Instant.now())
                    .items(releasedItems)
                    .build();

            String payloadJson = objectMapper.writeValueAsString(inventoryReleasedEvent);

            kafkaTemplate.send(inventoryEventsTopic, event.getOrderNumber(), payloadJson)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("BƯỚC 3 (Inventory Service): ĐÃ BẮN EVENT INVENTORY_RELEASED thành công cho đơn [{}] vào topic [{}]",
                                    event.getOrderNumber(), inventoryEventsTopic);
                        } else {
                            log.error("Lỗi khi bắn event INVENTORY_RELEASED vào Kafka: {}", ex.getMessage(), ex);
                        }
                    });

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            idempotencyBucket.delete();
            throw new RuntimeException("Thread bị ngắt khi chờ Distributed Lock", e);
        } catch (Exception e) {
            log.error("Lỗi khi giải phóng tồn kho cho đơn [{}]: {}", event.getOrderNumber(), e.getMessage(), e);
            idempotencyBucket.delete();
            throw new RuntimeException("Thất bại khi hoàn trả tồn kho", e);
        } finally {
            // 7. Giải phóng Redisson Distributed Lock an toàn
            if (multiLock.isHeldByCurrentThread()) {
                multiLock.unlock();
                log.info("Đã mở khóa (unlock) Redisson Distributed MultiLock cho các SKUs {}", sortedSkus);
            }
        }
    }
}
