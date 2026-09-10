package com.xcare.order.dto.request;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderRequest {

    @NotBlank(message = "Customer ID must not be blank")
    private String customerId;

    @NotBlank(message = "Pharmacy Hub ID must not be blank")
    private String pharmacyHubId;

    @NotBlank(message = "Payment method must not be blank")
    @Pattern(regexp = "COD|VNPAY|MOMO|ZALOPAY|BANK_TRANSFER", message = "Invalid payment method")
    private String paymentMethod;

    @NotBlank(message = "Recipient name must not be blank")
    private String recipientName;

    @NotBlank(message = "Recipient phone must not be blank")
    @Pattern(regexp = "^(0|\\+84)[3|5|7|8|9][0-9]{8}$", message = "Invalid Vietnamese phone number format")
    private String recipientPhone;

    @NotBlank(message = "Shipping address must not be blank")
    private String shippingAddress;

    private String note;

    @NotEmpty(message = "Order must contain at least one item")
    @Valid
    private List<OrderItemRequest> items;
}
