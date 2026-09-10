package com.xcare.order.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * Domain Event published when an order is successfully created and inventory is reserved.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {

    private UUID eventId;
    private Instant eventTimestamp;
    private UUID orderId;
    private String orderNumber;
    private String customerId;
    private String pharmacyHubId;
    private BigDecimal finalAmount;
    private String paymentMethod;
    private String paymentStatus;
    private String recipientName;
    private String recipientPhone;
    private String shippingAddress;
    private List<OrderItemEventPayload> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderItemEventPayload {
        private UUID itemId;
        private String sku;
        private String productName;
        private Integer quantity;
        private BigDecimal unitPrice;
        private BigDecimal totalPrice;
        private String batchNumber;
        private LocalDate expiryDate;
        private Boolean requiresPrescription;
    }
}
