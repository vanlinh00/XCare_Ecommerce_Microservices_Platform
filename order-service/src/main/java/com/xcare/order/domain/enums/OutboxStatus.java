package com.xcare.order.domain.enums;

/**
 * Processing state of an Outbox Event.
 */
public enum OutboxStatus {
    PENDING,
    PUBLISHED,
    FAILED
}
