package com.xcare.order.service;

import com.xcare.order.dto.request.CreateOrderRequest;
import com.xcare.order.dto.response.OrderResponse;

import java.util.UUID;

public interface OrderService {

    /**
     * Create an order with distributed lock reservation and transactional outbox persistence.
     *
     * @param request Client creation payload
     * @return Created order representation
     */
    OrderResponse createOrder(CreateOrderRequest request);

    /**
     * Retrieve order details by ID.
     *
     * @param orderId UUID of the order
     * @return Order response
     */
    OrderResponse getOrderById(UUID orderId);

    /**
     * Bước 1 trong Saga: Khách hàng ấn Hủy đơn.
     * Đổi trạng thái sang CANCEL_REQUESTED và lưu event vào outbox_events trong cùng DB Transaction.
     *
     * @param orderId ID đơn hàng
     * @param request Lý do và người yêu cầu
     * @return Kết quả tiếp nhận hủy đơn
     */
    com.xcare.order.dto.response.CancelOrderResponse cancelOrder(UUID orderId, com.xcare.order.dto.request.CancelOrderRequest request);

    /**
     * Bước 4 trong Saga: Nhận Event INVENTORY_RELEASED từ Kafka.
     * Cập nhật trạng thái đơn thành CANCELLED_BY_CUSTOMER và đóng Saga Process.
     *
     * @param event Dữ liệu hoàn kho thành công từ Inventory Service
     */
    void completeOrderCancellation(com.xcare.order.event.InventoryReleasedEvent event);
}
