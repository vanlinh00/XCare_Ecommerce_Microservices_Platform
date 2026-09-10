package com.xcare.fulfillment.pack.controller;

import com.xcare.fulfillment.pack.dto.CancelPackRequest;
import com.xcare.fulfillment.pack.dto.CancelPackResponse;
import com.xcare.fulfillment.pack.service.FulfillmentPackService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller phục vụ Dược sĩ / Nhân viên kho thực hiện thao tác đóng gói & xử lý sự cố.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/fulfillment")
@RequiredArgsConstructor
public class FulfillmentController {

    private final FulfillmentPackService fulfillmentPackService;

    /**
     * BƯỚC 1 (Task 2): Dược sĩ tại kho phát hiện thuốc hỏng hoặc hết hàng khi đóng gói, bấm "HỦY ĐÓNG GÓI".
     *
     * @param request Dữ liệu lý do hủy và danh sách SKU lỗi
     * @return Phản hồi tiếp nhận Saga
     */
    @PostMapping("/cancel-pack")
    public ResponseEntity<CancelPackResponse> cancelPacking(@RequestBody CancelPackRequest request) {
        log.info("Nhận API yêu cầu HỦY ĐÓNG GÓI cho đơn hàng: [{}]", request.getOrderNumber());
        CancelPackResponse response = fulfillmentPackService.cancelPackaging(request);
        return ResponseEntity.ok(response);
    }
}
