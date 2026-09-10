package com.xcare.order.domain.enums;

/**
 * Lifecycle states of an Order within the XCare Omnichannel Platform.
 */
public enum OrderStatus {
    CREATED,
    PENDING,
    PAYMENT_PENDING,
    CONFIRMED,
    FULFILLING,
    SHIPPED,
    DELIVERED,
    CANCEL_REQUESTED,           // Saga step 1: Request cancel submitted, rollback in progress
    CANCELLED_BY_CUSTOMER,      // Saga step 4: Rollback finished, stock released & 3PL cancelled
    CANCELLED_OUT_OF_STOCK,     // Task 2: Saga Rollback when Fulfillment/Inventory packaging fails
    REVERTING_INVENTORY,        // Step 4 of Orchestration Saga: Shipping failed -> Reverting inventory
    ORDER_FAILED_SHIPPING_ERROR,// Step 6 of Orchestration Saga: Inventory released -> Order terminated with shipping error
    CANCELLED_BY_SYSTEM,
    CANCELLED,
    RETURNED
}
