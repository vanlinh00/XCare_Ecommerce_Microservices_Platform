package com.xcare.fulfillment.pack.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Saga Event phát sinh từ Bước 1: Dược sĩ bấm "HỦY ĐÓNG GÓI" do thuốc bị hỏng / hết hàng trên kệ.
 * Bắn vào Kafka Topic: 'fulfillment-events'
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FulfillmentFailedEvent {

    private UUID eventId;
    private String sagaId;
    private UUID orderId;
    private String orderNumber;
    private String pharmacyHubId;
    private String pharmacistId;
    private String failedReason;
    private String status; // "FULFILLMENT_FAILED"
    private Instant failedAt;
    private List<FailedItemPayload> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FailedItemPayload {
        private String sku;
        private String productName;
        private Integer quantity;
        private boolean damaged;
        private String defectNote;
    }
}
