package com.xcare.shipping.client.impl;

import com.xcare.shipping.client.ThirdPartyLogisticsClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Adapter tích hợp các cổng 3PL: Ahamove (Giao hỏa tốc 1h - 2h từ nhà thuốc) và GHTK.
 */
@Slf4j
@Component
public class AhamoveGhtkClientImpl implements ThirdPartyLogisticsClient {

    @Value("${xcare.carriers.ahamove.api-url:https://api.ahamove.com/v1}")
    private String ahamoveApiUrl;

    @Value("${xcare.carriers.ghtk.api-url:https://services.giaohangtietkiem.vn}")
    private String ghtkApiUrl;

    @Override
    public Cancel3PLResponse cancelShipment(String carrier, String trackingCode, String reason) {
        log.info("Đang gọi API hủy vận đơn sang đối tác 3PL [{}] - TrackingCode [{}], Lý do: {}",
                carrier, trackingCode, reason);

        try {
            // Giả lập cuộc gọi HTTP REST API sang đối tác vận chuyển
            if ("AHAMOVE".equalsIgnoreCase(carrier)) {
                log.info("POST {}/order/cancel - Token: [PROTECTED], Shared_Order_ID: {}",
                        ahamoveApiUrl, trackingCode);
                // Ahamove API trả về status: CANCELLED, return code: 200
                return Cancel3PLResponse.builder()
                        .success(true)
                        .carrier("AHAMOVE")
                        .trackingCode(trackingCode)
                        .thirdPartyStatusCode("CANCELLED")
                        .message("Tài xế Ahamove đã nhận lệnh hủy chuyến và đổi trạng thái hoàn trả.")
                        .refundFee(0.0)
                        .build();

            } else if ("GHTK".equalsIgnoreCase(carrier)) {
                log.info("POST {}/services/shipment/cancel - Partner_Code: {}",
                        ghtkApiUrl, trackingCode);
                return Cancel3PLResponse.builder()
                        .success(true)
                        .carrier("GHTK")
                        .trackingCode(trackingCode)
                        .thirdPartyStatusCode("CANCEL_SUCCESS")
                        .message("Đơn hàng GHTK đã được hủy trước khi nhập kho trung chuyển.")
                        .refundFee(0.0)
                        .build();
            } else {
                // Fallback carrier mặc định
                log.info("Hủy đơn vận chuyển với đơn vị vận tải nội bộ/đối tác khác: {}", carrier);
                return Cancel3PLResponse.builder()
                        .success(true)
                        .carrier(carrier)
                        .trackingCode(trackingCode)
                        .thirdPartyStatusCode("SUCCESS")
                        .message("Hủy vận đơn thành công")
                        .refundFee(0.0)
                        .build();
            }
        } catch (Exception e) {
            log.error("Lỗi khi kết nối tới hệ thống 3PL [{}]: {}", carrier, e.getMessage(), e);
            return Cancel3PLResponse.builder()
                    .success(false)
                    .carrier(carrier)
                    .trackingCode(trackingCode)
                    .thirdPartyStatusCode("3PL_ERROR")
                    .message("Lỗi kết nối đối tác 3PL: " + e.getMessage())
                    .build();
        }
    }
}
