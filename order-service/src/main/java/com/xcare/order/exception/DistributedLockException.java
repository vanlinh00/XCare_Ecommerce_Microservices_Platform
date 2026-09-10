package com.xcare.order.exception;

public class DistributedLockException extends RuntimeException {
    public DistributedLockException(String message) {
        super(message);
    }
}
