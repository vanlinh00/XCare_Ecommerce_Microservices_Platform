package com.xcare.procurement.controller;

import com.xcare.procurement.domain.entity.StockTransferSaga;
import com.xcare.procurement.dto.CreateStockTransferRequest;
import com.xcare.procurement.dto.StockTransferResponse;
import com.xcare.procurement.saga.StockTransferSagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Controller tiếp nhận yêu cầu điều chuyển kho liên chi nhánh từ Quản lý chuỗi cung ứng (Procurement/Supply Chain Manager).
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/stock-transfers")
@RequiredArgsConstructor
public class StockTransferController {

    private final StockTransferSagaOrchestrator sagaOrchestrator;

    /**
     * Kích hoạt Saga Điều chuyển kho (Stock Transfer Saga)
     * POST /api/v1/stock-transfers
     */
    @PostMapping
    public ResponseEntity<StockTransferResponse> createStockTransfer(@RequestBody CreateStockTransferRequest request) {
        log.info("[STOCK-TRANSFER-API] Tiếp nhận yêu cầu điều chuyển kho từ [{}] sang [{}], lý do: [{}]",
                request.getFromHubId(), request.getToHubId(), request.getReason());

        StockTransferResponse response = sagaOrchestrator.startStockTransferSaga(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    /**
     * Tra cứu trạng thái Saga Điều chuyển kho
     * GET /api/v1/stock-transfers/{transferId}
     */
    @GetMapping("/{transferId}")
    public ResponseEntity<StockTransferSaga> getStockTransferSaga(@PathVariable String transferId) {
        StockTransferSaga saga = sagaOrchestrator.getTransferSaga(transferId);
        return ResponseEntity.ok(saga);
    }
}
