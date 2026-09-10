package com.xcare.shipping.event;

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
public class ShippingBookingFailedEvent implements Serializable {

    private UUID eventId;
    private UUID orderId;
    private String orderNumber;
    private String pharmacyHubId;
    private String carrier;
    private String failureReason;
    private int retryCount;
    private List<ItemPayload> items;
    private Instant failedAt;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ItemPayload implements Serializable {
        private String sku;
        private String productName;
        private Integer quantity;
    }
}
