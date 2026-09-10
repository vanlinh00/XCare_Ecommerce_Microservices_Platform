package com.xcare.fulfillment.inventory.controller;

import com.xcare.fulfillment.inventory.domain.HubStock;
import com.xcare.fulfillment.inventory.repository.HubStockRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/api/v1/inventory")
@RequiredArgsConstructor
public class InventoryController {

    private final HubStockRepository hubStockRepository;

    @GetMapping("/hubs/{hubId}/skus/{sku}")
    public ResponseEntity<?> getStock(
            @PathVariable("hubId") String hubId,
            @PathVariable("sku") String sku
    ) {
        Optional<HubStock> stock = hubStockRepository.findByHubIdAndSku(hubId, sku);
        return stock.map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Hub Fulfillment & Inventory Service is operational");
    }
}
