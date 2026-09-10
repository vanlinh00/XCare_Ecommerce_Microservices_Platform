package com.xcare.order.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderResponse {

    private UUID orderId;
    private String orderNumber;
    private String customerId;
    private String pharmacyHubId;
    private String status;
    private String paymentStatus;
    private String paymentMethod;
    private BigDecimal subtotalAmount;
    private BigDecimal discountAmount;
    private BigDecimal shippingFee;
    private BigDecimal finalAmount;
    private String recipientName;
    private String recipientPhone;
    private String shippingAddress;
    private String note;
    private Instant createdAt;
    private List<OrderItemResponse> items;
}
