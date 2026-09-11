package com.xcare.fulfillment.inventory.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Event bắn về Topic 'inventory-saga-responses' cho Procurement Service (:8084).
 * Xác nhận đã hoàn tất cộng tồn kho vật lý tại Hub đích an toàn với Redisson MultiLock.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StockTransferredEvent implements Serializable {

    private UUID eventId;
    private String eventType; // "STOCK_TRANSFERRED"
    private String sagaId;
    private String transferId;
    private String fromHubId;
    private String toHubId;
    private String status; // "INVENTORY_UPDATED"
    private Instant transferredAt;
    private List<TransferredItemPayload> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TransferredItemPayload implements Serializable {
        private String sku;
        private Integer addedQuantity;
        private Integer newAvailableQuantity;
    }
}
