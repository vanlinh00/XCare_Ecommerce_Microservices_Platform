package com.xcare.procurement.domain.entity;

import com.xcare.procurement.domain.enums.TransferSagaStatus;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * Entity quản lý trạng thái của Saga Điều chuyển kho (Stock Transfer Saga State Machine).
 */
@Entity
@Table(name = "stock_transfer_sagas")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockTransferSaga {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "transfer_id", nullable = false, unique = true, length = 64)
    private String transferId;

    @Column(name = "from_hub_id", nullable = false, length = 64)
    private String fromHubId;

    @Column(name = "to_hub_id", nullable = false, length = 64)
    private String toHubId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private TransferSagaStatus status;

    @Column(name = "reason", length = 255)
    private String reason;

    @Column(name = "requested_by", length = 64)
    private String requestedBy;

    @Column(name = "items_json", columnDefinition = "TEXT")
    private String itemsJson;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;
}
