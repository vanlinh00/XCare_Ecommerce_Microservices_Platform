package com.xcare.shipping.controller;

import com.xcare.shipping.domain.entity.Shipment;
import com.xcare.shipping.repository.ShipmentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/shipments")
@RequiredArgsConstructor
public class ShippingController {

    private final ShipmentRepository shipmentRepository;

    @GetMapping("/order/{orderId}")
    public ResponseEntity<?> getShipmentByOrderId(@PathVariable UUID orderId) {
        Optional<Shipment> shipment = shipmentRepository.findByOrderId(orderId);
        return shipment.map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Shipping & 3PL Integration Service is operational");
    }
}
