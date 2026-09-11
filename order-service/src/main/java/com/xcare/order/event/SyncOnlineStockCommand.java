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
 * BƯỚC 3 trong Task 4: Command gửi từ Procurement Service (:8084) sang Topic 'catalog-commands'.
 * Yêu cầu Order/Catalog Service (:8081) mở bán online lại các SKU vừa được điều chuyển nhập kho.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SyncOnlineStockCommand implements Serializable {

    private UUID commandId;
    private String sagaId;
    private String transferId;
    private String targetHubId;
    private String reason;
    private Instant createdAt;
    private List<CatalogSyncItemPayload> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CatalogSyncItemPayload implements Serializable {
        private String sku;
        private Integer addedQuantity;
        private Integer totalAvailableQuantity;
    }
}
