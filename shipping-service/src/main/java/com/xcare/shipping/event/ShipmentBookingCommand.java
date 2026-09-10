package com.xcare.shipping.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShipmentBookingCommand implements Serializable {

    private UUID orderId;
    private String orderNumber;
    private String pharmacyHubId;
    private String carrier; // AHAMOVE, GHTK, etc.
    private String deliveryAddress;
    private String customerPhone;
    private List<ItemPayload> items;

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
