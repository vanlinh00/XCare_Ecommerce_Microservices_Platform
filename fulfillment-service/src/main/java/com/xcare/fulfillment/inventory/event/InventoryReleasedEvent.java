package com.xcare.fulfillment.inventory.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Saga Event: Bắn ra từ Inventory Service sau khi hoàn trả tồn kho thành công (Bước 3).
 * Bắn sang Kafka Topic 'inventory-events'.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryReleasedEvent {

    private UUID eventId;
    private String sagaId;
    private UUID orderId;
    private String orderNumber;
    private String pharmacyHubId;
    private String status; // "INVENTORY_RELEASED"
    private String cancellationReason;
    private String sagaType; // "CUSTOMER_CANCEL" or "FULFILLMENT_FAILED_OUT_OF_STOCK"
    private Instant releasedAt;
    private List<ReleasedItemPayload> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReleasedItemPayload {
        private String sku;
        private Integer releasedQuantity;
    }
}
