package com.xcare.procurement.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateStockTransferRequest implements Serializable {

    private String fromHubId;
    private String toHubId;
    private String reason;
    private String requestedBy;
    private List<TransferItemRequest> items;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TransferItemRequest implements Serializable {
        private String sku;
        private String productName;
        private Integer quantity;
    }
}
