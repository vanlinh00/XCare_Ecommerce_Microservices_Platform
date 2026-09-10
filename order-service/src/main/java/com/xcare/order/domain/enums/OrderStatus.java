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
    CANCEL_REQUESTED,           // Saga step 1: Request cancel submitted, rollback in progress
    CANCELLED_BY_CUSTOMER,      // Saga step 4: Rollback finished, stock released & 3PL cancelled
    CANCELLED_BY_SYSTEM,
    CANCELLED,
    RETURNED
}
