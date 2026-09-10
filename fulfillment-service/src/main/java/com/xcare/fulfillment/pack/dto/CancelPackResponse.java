package com.xcare.fulfillment.pack.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CancelPackResponse {

    private UUID orderId;
    private String orderNumber;
    private String sagaId;
    private String status; // "FULFILLMENT_FAILED"
    private String message;
    private Instant cancelledAt;
}
