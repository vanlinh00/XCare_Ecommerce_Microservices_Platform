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
 * Command gửi từ Order Saga Orchestrator tới Inventory Service để hoàn tác kho Atomic.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RevertInventoryCommand implements Serializable {

    private UUID commandId;
    private String sagaId;
    private UUID orderId;
    private String orderNumber;
    private String pharmacyHubId;
    private String reason;
    private Instant createdAt;
    private List<RevertItemPayload> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RevertItemPayload implements Serializable {
        private String sku;
        private String productName;
        private Integer quantity;
    }
}
