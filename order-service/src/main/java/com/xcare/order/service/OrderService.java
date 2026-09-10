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
}
