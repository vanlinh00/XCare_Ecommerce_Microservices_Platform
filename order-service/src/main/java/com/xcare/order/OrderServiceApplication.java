package com.xcare.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * XCare Order Service Application
 *
 * Microservice responsible for:
 * - Order lifecycle management (Create, Validate, Confirm, Cancel)
 * - Dynamic pricing and pharmacy hub inventory reservation
 * - Redisson Distributed Locking to prevent inventory overselling
 * - Transactional Outbox Pattern for guaranteed at-least-once message delivery to Apache Kafka 3.7.0
 *
 * @author XCare Principal Architecture Team
 */
@SpringBootApplication
@EnableScheduling
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
