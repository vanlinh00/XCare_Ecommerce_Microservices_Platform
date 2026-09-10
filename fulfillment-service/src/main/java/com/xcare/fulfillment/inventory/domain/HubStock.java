package com.xcare.fulfillment.inventory.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "hub_stocks", uniqueConstraints = {
        @UniqueConstraint(name = "uk_hub_sku", columnNames = {"hub_id", "sku"})
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HubStock {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "hub_id", nullable = false, length = 64)
    private String hubId;

    @Column(name = "sku", nullable = false, length = 64)
    private String sku;

    @Column(name = "product_name", length = 255)
    private String productName;

    @Column(name = "available_quantity", nullable = false)
    private Integer availableQuantity;

    @Column(name = "reserved_quantity", nullable = false)
    private Integer reservedQuantity;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @PrePersist
    @PreUpdate
    public void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
