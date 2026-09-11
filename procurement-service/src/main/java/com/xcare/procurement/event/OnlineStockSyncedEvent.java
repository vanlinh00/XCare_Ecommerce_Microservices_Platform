package com.xcare.procurement.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OnlineStockSyncedEvent implements Serializable {

    private UUID eventId;
    private String eventType;
    private String sagaId;
    private String transferId;
    private String targetHubId;
    private String status;
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
