package com.xcare.shipping.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Event nhận từ topic 'fulfillment-events' khi nhân viên kho hủy đóng gói (Task 2)
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
