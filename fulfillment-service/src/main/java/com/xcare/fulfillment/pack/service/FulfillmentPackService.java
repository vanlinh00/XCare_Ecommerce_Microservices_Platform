package com.xcare.fulfillment.pack.service;

import com.xcare.fulfillment.pack.dto.CancelPackRequest;
import com.xcare.fulfillment.pack.dto.CancelPackResponse;

public interface FulfillmentPackService {

    /**
     * BƯỚC 1 (Task 2): Dược sĩ tại kho bấm "HỦY ĐÓNG GÓI" do phát hiện thuốc hỏng hoặc hết hàng.
     * Lưu sự kiện FULFILLMENT_FAILED vào bảng Outbox trong cùng 1 Transaction.
     *
     * @param request Dữ liệu hủy đóng gói từ nhân viên kho
     * @return Phản hồi tiếp nhận hủy đóng gói & mã Saga Correlation
     */
    CancelPackResponse cancelPackaging(CancelPackRequest request);
}
