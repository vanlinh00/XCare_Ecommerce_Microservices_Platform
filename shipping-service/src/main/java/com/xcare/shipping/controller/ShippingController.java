package com.xcare.shipping.controller;

import com.xcare.shipping.event.ShipmentBookingCommand;
import com.xcare.shipping.service.ShippingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/shipping")
@RequiredArgsConstructor
public class ShippingController {

    private final ShippingService shippingService;

    @PostMapping("/book")
    public ResponseEntity<Map<String, Object>> bookShipment(@RequestBody ShipmentBookingCommand command) {
        shippingService.bookShipping(command);
        return ResponseEntity.ok(Map.of(
                "success", true,
                "orderNumber", command.getOrderNumber(),
                "message", "Shipping booking processed or fallback triggered if 3PL failed"
        ));
    }
}
