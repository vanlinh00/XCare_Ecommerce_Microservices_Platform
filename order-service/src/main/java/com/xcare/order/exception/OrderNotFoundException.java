package com.xcare.order.exception;

import java.util.UUID;

public class OrderNotFoundException extends RuntimeException {
    public OrderNotFoundException(UUID orderId) {
        super("Order not found with id: " + orderId);
    }

    public OrderNotFoundException(String orderNumber) {
        super("Order not found with order number: " + orderNumber);
    }
}
