package com.xcare.order.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.order.domain.entity.Order;
import com.xcare.order.domain.entity.OrderItem;
import com.xcare.order.domain.entity.OutboxEvent;
import com.xcare.order.domain.enums.OrderStatus;
import com.xcare.order.domain.enums.OutboxStatus;
import com.xcare.order.domain.enums.PaymentMethod;
import com.xcare.order.domain.enums.PaymentStatus;
import com.xcare.order.dto.request.CreateOrderRequest;
import com.xcare.order.dto.request.OrderItemRequest;
import com.xcare.order.dto.response.OrderItemResponse;
import com.xcare.order.dto.response.OrderResponse;
import com.xcare.order.event.OrderCreatedEvent;
import com.xcare.order.exception.OrderNotFoundException;
import com.xcare.order.lock.DistributedLockManager;
import com.xcare.order.repository.OrderRepository;
import com.xcare.order.repository.OutboxEventRepository;
import com.xcare.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Production implementation of OrderService.
 *
 * Enforces:
 * 1. Redisson Distributed Lock on pharmacy hub SKUs to prevent overselling.
 * 2. ACID Transaction bundling Order persistence + Outbox Event creation.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final DistributedLockManager distributedLockManager;
    private final ObjectMapper objectMapper;

    @Value("${xcare.outbox.topics.order-created:xcare.orders.created.v1}")
    private String orderCreatedTopic;

    @Override
    public OrderResponse createOrder(CreateOrderRequest request) {
        List<String> requestedSkus = request.getItems().stream()
                .map(OrderItemRequest::getSku)
                .toList();

        // Step 1: Secure Distributed Lock over requested SKUs at the specified pharmacy hub
        return distributedLockManager.executeWithInventoryLock(
                request.getPharmacyHubId(),
                requestedSkus,
                () -> saveOrderAndOutboxInTransaction(request)
        );
    }

    /**
     * Executes strictly within an ACID transaction.
     * Guarantees that Order state and Outbox event are atomically committed together.
     */
    @Transactional(rollbackFor = Exception.class)
    public OrderResponse saveOrderAndOutboxInTransaction(CreateOrderRequest request) {
        log.info("Processing order placement for customer [{}] at hub [{}]",
                request.getCustomerId(), request.getPharmacyHubId());

        // 1. Calculate financial aggregates (subtotal, shipping, discounts)
        BigDecimal subtotal = BigDecimal.ZERO;
        for (OrderItemRequest itemReq : request.getItems()) {
            BigDecimal lineTotal = itemReq.getUnitPrice().multiply(BigDecimal.valueOf(itemReq.getQuantity()));
            subtotal = subtotal.add(lineTotal);
        }

        BigDecimal discountAmount = BigDecimal.ZERO; // Extensible for voucher/coupon engine
        BigDecimal shippingFee = new BigDecimal("25000.00"); // Flat standard pharmaceutical courier rate (25k VND)
        BigDecimal finalAmount = subtotal.subtract(discountAmount).add(shippingFee);

        // 2. Generate unique order tracking code (e.g. XC-260909-XXXX)
        String orderNumber = generateUniqueOrderNumber();

        // 3. Build Order Aggregate
        Order order = Order.builder()
                .orderNumber(orderNumber)
                .customerId(request.getCustomerId())
                .pharmacyHubId(request.getPharmacyHubId())
                .status(OrderStatus.CREATED)
                .paymentStatus(PaymentStatus.PENDING)
                .paymentMethod(PaymentMethod.valueOf(request.getPaymentMethod()))
                .subtotalAmount(subtotal)
                .discountAmount(discountAmount)
                .shippingFee(shippingFee)
                .finalAmount(finalAmount)
                .recipientName(request.getRecipientName())
                .recipientPhone(request.getRecipientPhone())
                .shippingAddress(request.getShippingAddress())
                .note(request.getNote())
                .build();

        // 4. Attach line items with pharmaceutical regulatory data
        for (OrderItemRequest itemReq : request.getItems()) {
            BigDecimal lineTotal = itemReq.getUnitPrice().multiply(BigDecimal.valueOf(itemReq.getQuantity()));
            OrderItem item = OrderItem.builder()
                    .sku(itemReq.getSku())
                    .productName(itemReq.getProductName())
                    .quantity(itemReq.getQuantity())
                    .unitPrice(itemReq.getUnitPrice())
                    .totalPrice(lineTotal)
                    .batchNumber(itemReq.getBatchNumber())
                    .expiryDate(itemReq.getExpiryDate())
                    .requiresPrescription(Boolean.TRUE.equals(itemReq.getRequiresPrescription()))
                    .build();
            order.addItem(item);
        }

        // 5. Persist Order Aggregate (orders & order_items)
        Order savedOrder = orderRepository.save(order);
        log.info("Saved order with ID [{}] and Order Number [{}]", savedOrder.getId(), savedOrder.getOrderNumber());

        // 6. Build and persist Outbox Event in the SAME ACID Transaction
        OutboxEvent outboxEvent = buildOutboxEvent(savedOrder);
        outboxEventRepository.save(outboxEvent);
        log.info("Saved outbox event [{}] for aggregate [{}] to topic [{}]",
                outboxEvent.getId(), outboxEvent.getAggregateId(), outboxEvent.getTopic());

        return mapToOrderResponse(savedOrder);
    }

    @Override
    @Transactional(readOnly = true)
    public OrderResponse getOrderById(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        return mapToOrderResponse(order);
    }

    private OutboxEvent buildOutboxEvent(Order order) {
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .eventId(UUID.randomUUID())
                .eventTimestamp(Instant.now())
                .orderId(order.getId())
                .orderNumber(order.getOrderNumber())
                .customerId(order.getCustomerId())
                .pharmacyHubId(order.getPharmacyHubId())
                .finalAmount(order.getFinalAmount())
                .paymentMethod(order.getPaymentMethod().name())
                .paymentStatus(order.getPaymentStatus().name())
                .recipientName(order.getRecipientName())
                .recipientPhone(order.getRecipientPhone())
                .shippingAddress(order.getShippingAddress())
                .items(order.getItems().stream()
                        .map(i -> OrderCreatedEvent.OrderItemEventPayload.builder()
                                .itemId(i.getId())
                                .sku(i.getSku())
                                .productName(i.getProductName())
                                .quantity(i.getQuantity())
                                .unitPrice(i.getUnitPrice())
                                .totalPrice(i.getTotalPrice())
                                .batchNumber(i.getBatchNumber())
                                .expiryDate(i.getExpiryDate())
                                .requiresPrescription(i.getRequiresPrescription())
                                .build())
                        .toList())
                .build();

        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize OrderCreatedEvent to JSON", e);
            throw new IllegalStateException("Failed to serialize outbox event payload", e);
        }

        return OutboxEvent.builder()
                .aggregateType("ORDER")
                .aggregateId(order.getId().toString())
                .eventType(OrderCreatedEvent.class.getSimpleName())
                .topic(orderCreatedTopic)
                .partitionKey(order.getOrderNumber()) // OrderNumber as partition key preserves strict partition order in Kafka
                .payload(payloadJson)
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .build();
    }

    private OrderResponse mapToOrderResponse(Order order) {
        return OrderResponse.builder()
                .orderId(order.getId())
                .orderNumber(order.getOrderNumber())
                .customerId(order.getCustomerId())
                .pharmacyHubId(order.getPharmacyHubId())
                .status(order.getStatus().name())
                .paymentStatus(order.getPaymentStatus().name())
                .paymentMethod(order.getPaymentMethod().name())
                .subtotalAmount(order.getSubtotalAmount())
                .discountAmount(order.getDiscountAmount())
                .shippingFee(order.getShippingFee())
                .finalAmount(order.getFinalAmount())
                .recipientName(order.getRecipientName())
                .recipientPhone(order.getRecipientPhone())
                .shippingAddress(order.getShippingAddress())
                .note(order.getNote())
                .createdAt(order.getCreatedAt())
                .items(order.getItems().stream()
                        .map(i -> OrderItemResponse.builder()
                                .itemId(i.getId())
                                .sku(i.getSku())
                                .productName(i.getProductName())
                                .quantity(i.getQuantity())
                                .unitPrice(i.getUnitPrice())
                                .totalPrice(i.getTotalPrice())
                                .batchNumber(i.getBatchNumber())
                                .expiryDate(i.getExpiryDate())
                                .requiresPrescription(i.getRequiresPrescription())
                                .build())
                        .toList())
                .build();
    }

    private String generateUniqueOrderNumber() {
        long timestamp = System.currentTimeMillis() % 1000000000L;
        int randomSuffix = ThreadLocalRandom.current().nextInt(1000, 9999);
        return String.format("XC-%d-%d", timestamp, randomSuffix);
    }
}
