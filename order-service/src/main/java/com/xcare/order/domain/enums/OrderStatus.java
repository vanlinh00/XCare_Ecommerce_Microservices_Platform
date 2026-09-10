package com.xcare.order.domain.enums;

/**
 * Lifecycle states of an Order within the XCare Omnichannel Platform.
 */
public enum OrderStatus {
    CREATED,
    PAYMENT_PENDING,
    CONFIRMED,
    FULFILLING,
    SHIPPED,
    DELIVERED,
    CANCELLED,
    RETURNED
}
