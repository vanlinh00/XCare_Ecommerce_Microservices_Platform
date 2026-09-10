package com.xcare.shipping.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Event nhận từ topic 'order-cancellation-events'
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCancelRequestedEvent {

    private UUID eventId;
    private String sagaId;
    private UUID orderId;
    private String orderNumber;
    private String customerId;
    private String pharmacyHubId;
    private String cancelReason;
    private String requestedBy;
    private Instant requestedAt;
    private List<CancelItemPayload> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CancelItemPayload {
        private String sku;
        private String productName;
        private Integer quantity;
    }
}
