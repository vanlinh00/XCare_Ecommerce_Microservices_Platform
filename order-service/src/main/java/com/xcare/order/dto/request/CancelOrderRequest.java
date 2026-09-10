package com.xcare.order.dto.request;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Payload yêu cầu hủy đơn thuốc từ khách hàng qua Mobile App / Web.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CancelOrderRequest {

    @NotBlank(message = "Lý do hủy đơn không được để trống")
    private String reason;

    private String cancelledBy; // e.g., "CUSTOMER" or Customer ID
}
