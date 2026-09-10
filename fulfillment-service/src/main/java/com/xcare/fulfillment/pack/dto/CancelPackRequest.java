package com.xcare.fulfillment.pack.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.UUID;

/**
 * Request DTO gửi từ Giao diện Dược sĩ / Nhân viên kho khi bấm "HỦY ĐÓNG GÓI"
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CancelPackRequest {

    private UUID orderId;
    private String orderNumber;
    private String pharmacyHubId;
    private String pharmacistId;
    private String reason; // Ví dụ: "Thuốc bị vỡ ống/ẩm mốc", "Hết hàng thực tế tại kệ kho"
    private List<CancelPackItem> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CancelPackItem {
        private String sku;
        private String productName;
        private Integer quantity;
        private boolean damaged;
        private String defectNote;
    }
}
