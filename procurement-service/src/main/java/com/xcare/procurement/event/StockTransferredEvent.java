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
public class StockTransferredEvent implements Serializable {

    private UUID eventId;
    private String eventType;
    private String sagaId;
    private String transferId;
    private String fromHubId;
    private String toHubId;
    private String status;
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
