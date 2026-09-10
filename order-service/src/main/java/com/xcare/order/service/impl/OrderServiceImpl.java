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
import com.xcare.order.dto.request.CancelOrderRequest;
import com.xcare.order.dto.request.CreateOrderRequest;
import com.xcare.order.dto.request.OrderItemRequest;
import com.xcare.order.dto.response.CancelOrderResponse;
import com.xcare.order.dto.response.OrderItemResponse;
import com.xcare.order.dto.response.OrderResponse;
import com.xcare.order.event.InventoryReleasedEvent;
import com.xcare.order.event.OrderCancelRequestedEvent;
import com.xcare.order.event.OrderCreatedEvent;
import com.xcare.order.exception.IllegalOrderStateException;
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

    @Value("${xcare.outbox.topics.order-cancellation:order-cancellation-events}")
    private String orderCancellationTopic;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CancelOrderResponse cancelOrder(UUID orderId, CancelOrderRequest request) {
        log.info("BƯỚC 1 (Order Service): Tiếp nhận yêu cầu hủy đơn [{}] từ [{}] với lý do: {}",
                orderId, request.getCancelledBy(), request.getReason());

        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        // 1. Kiểm tra trạng thái đơn: Phải là trạng thái cho phép hủy (Shipper đang tới lấy, đang chuẩn bị thuốc, mới tạo)
        if (order.getStatus() == OrderStatus.DELIVERED || order.getStatus() == OrderStatus.RETURNED) {
            throw new IllegalOrderStateException("Đơn hàng đã được giao thành công hoặc đã hoàn tất, không thể hủy!");
        }

        if (order.getStatus() == OrderStatus.CANCELLED_BY_CUSTOMER || order.getStatus() == OrderStatus.CANCELLED) {
            log.warn("Đơn hàng [{}] đã ở trạng thái hủy từ trước", orderId);
            return CancelOrderResponse.builder()
                    .orderId(order.getId())
                    .orderNumber(order.getOrderNumber())
                    .status(order.getStatus().name())
                    .message("Đơn hàng đã được hủy trước đó")
                    .requestedAt(Instant.now())
                    .build();
        }

        if (order.getStatus() == OrderStatus.CANCEL_REQUESTED) {
            log.warn("Đơn hàng [{}] đang trong tiến trình Saga Rollback", orderId);
            return CancelOrderResponse.builder()
                    .orderId(order.getId())
                    .orderNumber(order.getOrderNumber())
                    .status(OrderStatus.CANCEL_REQUESTED.name())
                    .message("Yêu cầu hủy đơn đang được xử lý (Saga in-progress)")
                    .requestedAt(Instant.now())
                    .build();
        }

        // 2. Chuyển trạng thái sang CANCEL_REQUESTED (Saga Step 1)
        order.setStatus(OrderStatus.CANCEL_REQUESTED);
        order.setNote((order.getNote() != null ? order.getNote() + " | " : "") +
                "[Yêu cầu hủy]: " + request.getReason() + " (Bởi: " + request.getCancelledBy() + ")");
        orderRepository.save(order);

        // 3. Khởi tạo Saga Rollback Event & ghi vào bảng outbox_events trong CÙNG DB Transaction
        String sagaId = "SAGA-ROLLBACK-" + order.getOrderNumber() + "-" + UUID.randomUUID().toString().substring(0, 8);
        OutboxEvent outboxEvent = buildCancelOrderOutboxEvent(order, sagaId, request);
        outboxEventRepository.save(outboxEvent);

        log.info("BƯỚC 1 (Order Service): Đã lưu Outbox Event [{}] cho Saga [{}] vào topic [{}]",
                outboxEvent.getId(), sagaId, outboxEvent.getTopic());

        return CancelOrderResponse.builder()
                .orderId(order.getId())
                .orderNumber(order.getOrderNumber())
                .sagaId(sagaId)
                .status(OrderStatus.CANCEL_REQUESTED.name())
                .message("Đã tiếp nhận yêu cầu hủy đơn. Hệ thống đang kích hoạt quy trình Saga Rollback.")
                .requestedAt(Instant.now())
                .build();
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void completeOrderCancellation(InventoryReleasedEvent event) {
        log.info("BƯỚC 4 (Order Service): Nhận xác nhận INVENTORY_RELEASED cho đơn [{}] từ Saga [{}]",
                event.getOrderNumber(), event.getSagaId());

        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow(() -> new OrderNotFoundException(event.getOrderId()));

        // Kiểm tra Idempotence: Nếu đơn đã hủy rồi thì không xử lý lại
        if (order.getStatus() == OrderStatus.CANCELLED_BY_CUSTOMER) {
            log.warn("Đơn [{}] đã được cập nhật CANCELLED_BY_CUSTOMER từ trước. Bỏ qua event lặp.", order.getOrderNumber());
            return;
        }

        // Cập nhật trạng thái cuối cùng thành CANCELLED_BY_CUSTOMER và đóng Saga Process
        order.setStatus(OrderStatus.CANCELLED_BY_CUSTOMER);
        order.setNote((order.getNote() != null ? order.getNote() + " | " : "") +
                "[Saga Completed]: Đã hủy 3PL & hoàn trả tồn kho thành công lúc " + Instant.now());
        orderRepository.save(order);

        log.info("BƯỚC 4 (Order Service): ĐÃ ĐÓNG SAGA THÀNH CÔNG cho đơn [{}]. Trạng thái cuối: CANCELLED_BY_CUSTOMER",
                order.getOrderNumber());
    }

    private OutboxEvent buildCancelOrderOutboxEvent(Order order, String sagaId, CancelOrderRequest request) {
        OrderCancelRequestedEvent event = OrderCancelRequestedEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(order.getId())
                .orderNumber(order.getOrderNumber())
                .customerId(order.getCustomerId())
                .pharmacyHubId(order.getPharmacyHubId())
                .cancelReason(request.getReason())
                .requestedBy(request.getCancelledBy() != null ? request.getCancelledBy() : "CUSTOMER")
                .requestedAt(Instant.now())
                .items(order.getItems().stream()
                        .map(i -> OrderCancelRequestedEvent.CancelItemPayload.builder()
                                .sku(i.getSku())
                                .productName(i.getProductName())
                                .quantity(i.getQuantity())
                                .build())
                        .toList())
                .build();

        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Không thể serialize OrderCancelRequestedEvent", e);
        }

        return OutboxEvent.builder()
                .aggregateType("ORDER_SAGA")
                .aggregateId(order.getId().toString())
                .eventType(OrderCancelRequestedEvent.class.getSimpleName())
                .topic(orderCancellationTopic)
                .partitionKey(order.getOrderNumber()) // Đảm bảo toàn bộ message cùng đơn vào chung 1 Kafka partition
                .payload(payloadJson)
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .build();
    }

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
