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
 * Command nhận từ Procurement Service (:8084) qua Topic 'inventory-commands'.
 * Yêu cầu điều chuyển và cộng tồn kho vật lý tại Hub đích.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferStockCommand implements Serializable {

    private UUID commandId;
    private String sagaId;
    private String transferId;
    private String fromHubId;
    private String toHubId;
    private String reason;
    private String requestedBy;
    private Instant createdAt;
    private List<TransferItemPayload> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TransferItemPayload implements Serializable {
        private String sku;
        private String productName;
        private Integer quantity;
    }
}
