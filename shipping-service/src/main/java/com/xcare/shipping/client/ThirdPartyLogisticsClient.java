package com.xcare.shipping.client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface ThirdPartyLogisticsClient {

    /**
     * Gọi API hủy vận đơn sang đối tác 3PL (Ahamove, GHTK, GHN, Viettel Post).
     *
     * @param carrier Tên đơn vị vận chuyển
     * @param trackingCode Mã vận đơn 3PL
     * @param reason Lý do hủy
     * @return Kết quả hủy từ 3PL
     */
    Cancel3PLResponse cancelShipment(String carrier, String trackingCode, String reason);

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    class Cancel3PLResponse {
        private boolean success;
        private String carrier;
        private String trackingCode;
        private String thirdPartyStatusCode;
        private String message;
        private Double refundFee;
    }
}
