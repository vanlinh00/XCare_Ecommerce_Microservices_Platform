package com.xcare.procurement.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StockTransferResponse implements Serializable {

    private String transferId;
    private String sagaId;
    private String fromHubId;
    private String toHubId;
    private String status;
    private String message;
    private Instant createdAt;
}
