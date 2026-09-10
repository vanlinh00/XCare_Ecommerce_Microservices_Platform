package com.xcare.order.controller;

import com.xcare.order.dto.request.CreateOrderRequest;
import com.xcare.order.dto.response.ApiResponse;
import com.xcare.order.dto.response.OrderResponse;
import com.xcare.order.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    /**
     * Create a new omnichannel pharmacy order.
     * Enforces Redisson distributed lock over inventory and writes to Outbox atomically.
     */
    @PostMapping
    public ResponseEntity<ApiResponse<OrderResponse>> createOrder(
            @Valid @RequestBody CreateOrderRequest request
    ) {
        log.info("Received request to create order for customer [{}] with {} items",
                request.getCustomerId(), request.getItems().size());

        OrderResponse response = orderService.createOrder(request);

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ApiResponse.success(response, "Order created successfully. Outbox event scheduled."));
    }

    /**
     * Retrieve order details by Order UUID.
     */
    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<OrderResponse>> getOrderById(@PathVariable("id") UUID id) {
        OrderResponse response = orderService.getOrderById(id);
        return ResponseEntity.ok(ApiResponse.success(response, "Order retrieved successfully."));
    }

    /**
     * Lightweight health probe endpoint.
     */
    @GetMapping("/health")
    public ResponseEntity<ApiResponse<String>> healthCheck() {
        return ResponseEntity.ok(ApiResponse.success("Order Service is operational", "OK"));
    }
}
