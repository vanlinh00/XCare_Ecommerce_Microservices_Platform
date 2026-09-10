package com.xcare.order.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Phản hồi trạng thái tiếp nhận yêu cầu hủy đơn và tiến trình Saga Rollback.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CancelOrderResponse {

    private UUID orderId;
    private String orderNumber;
    private String sagaId;
    private String status;
    private String message;
    private Instant requestedAt;
}
