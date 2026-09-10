package com.xcare.shipping.domain.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "shipments", indexes = {
        @Index(name = "idx_shipment_order_id", columnList = "order_id"),
        @Index(name = "idx_shipment_tracking_code", columnList = "tracking_code")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Shipment {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "order_number", nullable = false, length = 64)
    private String orderNumber;

    @Column(name = "pharmacy_hub_id", nullable = false, length = 64)
    private String pharmacyHubId;

    @Column(name = "carrier", nullable = false, length = 32)
    private String carrier; // AHAMOVE, GHTK

    @Column(name = "tracking_code", nullable = false, unique = true, length = 64)
    private String trackingCode;

    @Column(name = "status", nullable = false, length = 32)
    private String status; // ASSIGNED, PICKING_UP, IN_TRANSIT, CANCELLED

    @Column(name = "cancellation_reason", length = 500)
    private String cancellationReason;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @PrePersist
    public void onPrePersist() {
        if (createdAt == null) createdAt = Instant.now();
        if (updatedAt == null) updatedAt = Instant.now();
    }

    @PreUpdate
    public void onPreUpdate() {
        updatedAt = Instant.now();
    }
}
