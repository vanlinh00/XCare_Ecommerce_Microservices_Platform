package com.xcare.procurement;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Procurement Service (:8084) - Central Saga Orchestrator cho luồng điều chuyển kho (Stock Transfer).
 * Quản lý State Machine: TRANSFER_REQUESTED -> INVENTORY_UPDATED -> COMPLETED.
 */
@EnableKafka
@EnableScheduling
@SpringBootApplication
public class ProcurementServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProcurementServiceApplication.class, args);
    }
}
