package com.xcare.fulfillment.inventory.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

/**
 * Entity lưu trữ Poison Pill (Tin nhắn độc) cách ly từ Dead-Letter Queue (DLQ)
 * cho Fulfillment / Inventory Service (:8083).
 * Phục vụ Audit Trail, Dashboard quan sát SRE, và tính năng Re-process (Retry thủ công).
 */
@Entity
@Table(name = "failed_inventory_commands", indexes = {
        @Index(name = "idx_failed_inv_transfer_id", columnList = "transfer_id"),
        @Index(name = "idx_failed_inv_order_number", columnList = "order_number"),
        @Index(name = "idx_failed_inv_status", columnList = "status")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FailedInventoryCommand {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false)
    private Long id;

    @Column(name = "transfer_id", length = 64)
    private String transferId;

    @Column(name = "order_number", length = 64)
    private String orderNumber;

    @Column(name = "topic", nullable = false, length = 128)
    private String topic;

    @Column(name = "kafka_partition")
    private Integer kafkaPartition;

    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    @Column(name = "payload", columnDefinition = "TEXT")
    private String payload;

    @Column(name = "exception_class", length = 255)
    private String exceptionClass;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Builder.Default
    @Column(name = "status", nullable = false, length = 32)
    private String status = "FAILED";

    @Builder.Default
    @Column(name = "retry_count", nullable = false)
    private Integer retryCount = 0;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;

    @PrePersist
    public void onCreate() {
        if (this.createdAt == null) {
            this.createdAt = Instant.now();
        }
        if (this.updatedAt == null) {
            this.updatedAt = Instant.now();
        }
        if (this.status == null) {
            this.status = "FAILED";
        }
        if (this.retryCount == null) {
            this.retryCount = 0;
        }
    }

    @PreUpdate
    public void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
