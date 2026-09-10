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
 * Saga Response Event nhận từ Shipping Service (:8082) qua topic 'order-saga-responses'.
 * Báo cáo kết quả hủy vận đơn đối tác 3PL thành công (Bước 2 hoàn tất).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShipmentCancelledEvent implements Serializable {

    private UUID eventId;
    private String sagaId;
    private UUID orderId;
    private String orderNumber;
    private String pharmacyHubId;
    private String trackingCode;
    private String carrier;
    private String status;
    private String cancelReason;
    private Instant cancelledAt;
    private List<ShippingItemPayload> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ShippingItemPayload implements Serializable {
        private String sku;
        private String productName;
        private Integer quantity;
    }
}
