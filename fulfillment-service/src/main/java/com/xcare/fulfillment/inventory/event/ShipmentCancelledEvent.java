package com.xcare.fulfillment.inventory.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Event nhận từ topic 'shipping-cancellation-events'
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShipmentCancelledEvent {

    private UUID eventId;
    private String sagaId;
    private UUID orderId;
    private String orderNumber;
    private String pharmacyHubId;
    private String trackingCode;
    private String carrier;
    private String status;
    private String cancelReason;
    private Instant cancelledAt;
    private List<ShippingItemPayload> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ShippingItemPayload {
        private String sku;
        private String productName;
        private Integer quantity;
    }
}
