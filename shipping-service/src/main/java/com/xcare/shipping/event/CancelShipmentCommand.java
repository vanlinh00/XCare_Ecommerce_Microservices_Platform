package com.xcare.shipping.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Command nhận từ Order Saga Orchestrator (:8081) qua Kafka Topic 'shipping-commands'.
 * Yêu cầu hủy cuốc xe 3PL (Ahamove / GHTK) đã gán cho đơn thuốc.
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
    private String carrier;
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
