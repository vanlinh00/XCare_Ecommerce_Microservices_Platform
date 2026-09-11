package com.xcare.order.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * BƯỚC 4 trong Task 4: Response Event bắn về Topic 'inventory-saga-responses' cho Procurement Service (:8084).
 * Báo hiệu Order/Catalog Service (:8081) đã đồng bộ hiển thị và mở bán Online thành công.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OnlineStockSyncedEvent implements Serializable {

    private UUID eventId;
    private String eventType; // "ONLINE_STOCK_SYNCED"
    private String sagaId;
    private String transferId;
    private String targetHubId;
    private String status; // "COMPLETED"
    private Instant syncedAt;
    private List<SyncedCatalogItemPayload> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SyncedCatalogItemPayload implements Serializable {
        private String sku;
        private boolean onlineAvailable;
        private Integer syncedQuantity;
    }
}
