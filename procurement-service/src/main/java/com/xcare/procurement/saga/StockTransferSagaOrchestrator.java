package com.xcare.procurement.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.procurement.domain.entity.ProcurementOutboxEvent;
import com.xcare.procurement.domain.entity.StockTransferSaga;
import com.xcare.procurement.domain.enums.OutboxStatus;
import com.xcare.procurement.domain.enums.TransferSagaStatus;
import com.xcare.procurement.dto.CreateStockTransferRequest;
import com.xcare.procurement.dto.StockTransferResponse;
import com.xcare.procurement.event.OnlineStockSyncedEvent;
import com.xcare.procurement.event.StockTransferredEvent;
import com.xcare.procurement.event.SyncOnlineStockCommand;
import com.xcare.procurement.event.TransferStockCommand;
import com.xcare.procurement.repository.ProcurementOutboxEventRepository;
import com.xcare.procurement.repository.StockTransferSagaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Saga Orchestrator trung tâm tại Procurement Service (:8084).
 * Quản lý State Machine luồng điều chuyển kho (Stock Transfer) theo Command-Response Star Topology:
 *
 * [BƯỚC 1] Phát TransferStockCommand sang 'inventory-commands'
 * [BƯỚC 3] Nhận StockTransferredEvent -> Đổi trạng thái sang INVENTORY_UPDATED -> Phát SyncOnlineStockCommand sang 'catalog-commands'
 * [BƯỚC 4] Nhận OnlineStockSyncedEvent -> Chốt trạng thái COMPLETED!
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StockTransferSagaOrchestrator {

    private final StockTransferSagaRepository sagaRepository;
    private final ProcurementOutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${xcare.topics.inventory-commands:inventory-commands}")
    private String inventoryCommandsTopic;

    @Value("${xcare.topics.catalog-commands:catalog-commands}")
    private String catalogCommandsTopic;

    /**
     * BƯỚC 1: Khởi động Stock Transfer Saga Orchestration.
     */
    @Transactional(rollbackFor = Exception.class)
    public StockTransferResponse startStockTransferSaga(CreateStockTransferRequest request) {
        String transferId = "TRF-" + System.currentTimeMillis();
        String sagaId = UUID.randomUUID().toString();

        log.info("[SAGA-ORCHESTRATOR][BƯỚC 1] Khởi động Stock Transfer Saga: transferId=[{}], sagaId=[{}], from=[{}] -> to=[{}]",
                transferId, sagaId, request.getFromHubId(), request.getToHubId());

        try {
            String itemsJson = objectMapper.writeValueAsString(request.getItems());

            // 1. Khởi tạo State Machine Saga
            StockTransferSaga saga = StockTransferSaga.builder()
                    .id(UUID.randomUUID())
                    .transferId(transferId)
                    .fromHubId(request.getFromHubId())
                    .toHubId(request.getToHubId())
                    .status(TransferSagaStatus.TRANSFER_REQUESTED)
                    .reason(request.getReason())
                    .requestedBy(request.getRequestedBy())
                    .itemsJson(itemsJson)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();

            sagaRepository.save(saga);

            // 2. Tạo TransferStockCommand
            List<TransferStockCommand.TransferItemPayload> commandItems = request.getItems().stream()
                    .map(i -> TransferStockCommand.TransferItemPayload.builder()
                            .sku(i.getSku())
                            .productName(i.getProductName())
                            .quantity(i.getQuantity())
                            .build())
                    .toList();

            TransferStockCommand command = TransferStockCommand.builder()
                    .commandId(UUID.randomUUID())
                    .sagaId(sagaId)
                    .transferId(transferId)
                    .fromHubId(request.getFromHubId())
                    .toHubId(request.getToHubId())
                    .reason(request.getReason())
                    .requestedBy(request.getRequestedBy())
                    .createdAt(Instant.now())
                    .items(commandItems)
                    .build();

            String payloadJson = objectMapper.writeValueAsString(command);

            // 3. Ghi Transactional Outbox (procurement_outbox_events)
            ProcurementOutboxEvent outboxEvent = ProcurementOutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("STOCK_TRANSFER_SAGA")
                    .aggregateId(transferId)
                    .eventType("TRANSFER_STOCK_COMMAND")
                    .topic(inventoryCommandsTopic)
                    .partitionKey(request.getToHubId())
                    .payload(payloadJson)
                    .status(OutboxStatus.PUBLISHED)
                    .retryCount(0)
                    .createdAt(Instant.now())
                    .publishedAt(Instant.now())
                    .build();

            outboxEventRepository.save(outboxEvent);

            // 4. Phát Command sang Topic 'inventory-commands'
            kafkaTemplate.send(inventoryCommandsTopic, transferId, payloadJson);

            log.info("[SAGA-ORCHESTRATOR][BƯỚC 1] Đã phát TransferStockCommand sang topic [{}] cho transferId [{}]",
                    inventoryCommandsTopic, transferId);

            return StockTransferResponse.builder()
                    .transferId(transferId)
                    .sagaId(sagaId)
                    .fromHubId(request.getFromHubId())
                    .toHubId(request.getToHubId())
                    .status(TransferSagaStatus.TRANSFER_REQUESTED.name())
                    .message("Stock transfer saga initiated successfully")
                    .createdAt(saga.getCreatedAt())
                    .build();

        } catch (Exception e) {
            log.error("[SAGA-ORCHESTRATOR] Lỗi khởi tạo Stock Transfer Saga: {}", e.getMessage(), e);
            throw new RuntimeException("Không thể khởi động Stock Transfer Saga", e);
        }
    }

    /**
     * BƯỚC 3: Nhận StockTransferredEvent từ Fulfillment (:8083).
     * Cập nhật Saga -> INVENTORY_UPDATED và phát SyncOnlineStockCommand sang 'catalog-commands'.
     */
    @Transactional(rollbackFor = Exception.class)
    public void handleStockTransferred(StockTransferredEvent event) {
        log.info("[SAGA-ORCHESTRATOR][BƯỚC 3] Nhận StockTransferredEvent từ Fulfillment Service: transferId=[{}]",
                event.getTransferId());

        StockTransferSaga saga = sagaRepository.findByTransferId(event.getTransferId())
                .orElseThrow(() -> new IllegalArgumentException("Không tìm thấy StockTransferSaga cho transferId: " + event.getTransferId()));

        saga.setStatus(TransferSagaStatus.INVENTORY_UPDATED);
        saga.setUpdatedAt(Instant.now());
        sagaRepository.save(saga);

        try {
            // Xây dựng SyncOnlineStockCommand sang Order / Catalog Service (:8081)
            List<SyncOnlineStockCommand.CatalogSyncItemPayload> syncItems = event.getItems().stream()
                    .map(i -> SyncOnlineStockCommand.CatalogSyncItemPayload.builder()
                            .sku(i.getSku())
                            .addedQuantity(i.getAddedQuantity())
                            .totalAvailableQuantity(i.getNewAvailableQuantity())
                            .build())
                    .toList();

            SyncOnlineStockCommand syncCommand = SyncOnlineStockCommand.builder()
                    .commandId(UUID.randomUUID())
                    .sagaId(event.getSagaId())
                    .transferId(event.getTransferId())
                    .targetHubId(event.getToHubId())
                    .reason("Stock transfer completed at Hub " + event.getToHubId())
                    .createdAt(Instant.now())
                    .items(syncItems)
                    .build();

            String payloadJson = objectMapper.writeValueAsString(syncCommand);

            // Ghi Outbox
            ProcurementOutboxEvent outboxEvent = ProcurementOutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("STOCK_TRANSFER_SAGA")
                    .aggregateId(event.getTransferId())
                    .eventType("SYNC_ONLINE_STOCK_COMMAND")
                    .topic(catalogCommandsTopic)
                    .partitionKey(event.getToHubId())
                    .payload(payloadJson)
                    .status(OutboxStatus.PUBLISHED)
                    .retryCount(0)
                    .createdAt(Instant.now())
                    .publishedAt(Instant.now())
                    .build();

            outboxEventRepository.save(outboxEvent);

            // Phát Command sang topic 'catalog-commands'
            kafkaTemplate.send(catalogCommandsTopic, event.getTransferId(), payloadJson);

            log.info("[SAGA-ORCHESTRATOR][BƯỚC 3] Đã chuyển Saga sang INVENTORY_UPDATED và phát SyncOnlineStockCommand sang topic [{}]",
                    catalogCommandsTopic);

        } catch (Exception e) {
            log.error("[SAGA-ORCHESTRATOR] Lỗi khi tạo SyncOnlineStockCommand: {}", e.getMessage(), e);
            throw new RuntimeException("Lỗi phát SyncOnlineStockCommand", e);
        }
    }

    /**
     * BƯỚC 4: Nhận OnlineStockSyncedEvent từ Order / Catalog Service (:8081).
     * Chốt trạng thái Saga -> COMPLETED!
     */
    @Transactional(rollbackFor = Exception.class)
    public void handleOnlineStockSynced(OnlineStockSyncedEvent event) {
        log.info("[SAGA-ORCHESTRATOR][BƯỚC 4] Nhận OnlineStockSyncedEvent từ Order/Catalog Service: transferId=[{}]",
                event.getTransferId());

        StockTransferSaga saga = sagaRepository.findByTransferId(event.getTransferId())
                .orElseThrow(() -> new IllegalArgumentException("Không tìm thấy StockTransferSaga cho transferId: " + event.getTransferId()));

        saga.setStatus(TransferSagaStatus.COMPLETED);
        saga.setUpdatedAt(Instant.now());
        sagaRepository.save(saga);

        log.info("[SAGA-ORCHESTRATOR][BƯỚC 4] >>> HOÀN TẤT TOÀN BỘ SAGA ĐIỀU CHUYỂN KHO (Task 4 COMPLETED)! transferId=[{}] <<<",
                event.getTransferId());
    }

    @Transactional(readOnly = true)
    public StockTransferSaga getTransferSaga(String transferId) {
        return sagaRepository.findByTransferId(transferId)
                .orElseThrow(() -> new IllegalArgumentException("Không tìm thấy transferId: " + transferId));
    }
}
