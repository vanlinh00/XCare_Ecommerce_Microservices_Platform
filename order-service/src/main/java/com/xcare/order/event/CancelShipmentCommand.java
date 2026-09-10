package com.xcare.order.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Command gửi từ Order Saga Orchestrator tới Shipping Service (:8082)
 * để yêu cầu hủy vận đơn 3PL (Ahamove / GHTK) theo Saga Orchestration Flow (Bước 1).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CancelShipmentCommand implements Serializable {

    private UUID commandId;
    private String sagaId;
    private UUID orderId;
    private String orderNumber;
    private String pharmacyHubId;
    private String carrier; // Ví dụ: "AHAMOVE", "GHTK"
    private String trackingCode;
    private String reason;
    private String cancelledBy;
    private Instant createdAt;
    private List<CancelItemPayload> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CancelItemPayload implements Serializable {
        private String sku;
        private String productName;
        private Integer quantity;
    }
}
