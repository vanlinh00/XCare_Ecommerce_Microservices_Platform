package com.xcare.shipping.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.shipping.client.ThirdPartyLogisticsClient;
import com.xcare.shipping.domain.entity.Shipment;
import com.xcare.shipping.domain.entity.ShippingOutboxEvent;
import com.xcare.shipping.event.CancelShipmentCommand;
import com.xcare.shipping.event.FulfillmentFailedEvent;
import com.xcare.shipping.event.OrderCancelRequestedEvent;
import com.xcare.shipping.event.ShipmentCancelledEvent;
import com.xcare.shipping.repository.ShipmentRepository;
import com.xcare.shipping.repository.ShippingOutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Xử lý bước 2 trong Saga Orchestration Flow: Hủy chuyến vận chuyển 3PL (Ahamove / GHTK).
 * - Đảm bảo tính Idempotent Consumer bằng Redis 7.2 (setIfAbsent với TTL 24h).
 * - Gọi API 3PL với Exception Handling.
 * - Transactional Outbox Pattern lưu vào PostgreSQL (shipping_outbox).
 * - Bắn Response Event SHIPMENT_CANCELLED về topic 'order-saga-responses'.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShippingCancellationService {

    private final ShipmentRepository shipmentRepository;
    private final ShippingOutboxEventRepository shippingOutboxEventRepository;
    private final ThirdPartyLogisticsClient thirdPartyLogisticsClient;
    private final RedissonClient redissonClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${xcare.topics.order-saga-responses:order-saga-responses}")
    private String orderSagaResponsesTopic;

    @Value("${xcare.topics.shipping-cancellation:shipping-cancellation-events}")
    private String shippingCancellationTopic;

    @Value("${xcare.topics.shipping-events:shipping-events}")
    private String shippingEventsTopic;

    private static final String IDEMPOTENCY_PREFIX = "xcare:idempotency:shipping:cancel:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    /**
     * BƯỚC 2 (Task 1 Saga Orchestration Flow):
     * Nhận CancelShipmentCommand từ Order Saga Orchestrator qua topic 'shipping-commands'.
     */
    @Transactional(rollbackFor = Exception.class)
    public void processCancelShipmentCommand(CancelShipmentCommand command) {
        String idempotencyKey = IDEMPOTENCY_PREFIX + command.getOrderId();
        log.info("[SHIPPING-SERVICE][BƯỚC 2] Tiếp nhận CancelShipmentCommand cho đơn [{}], SagaId [{}]",
                command.getOrderNumber(), command.getSagaId());

        // 1. Kiểm tra tính Idempotent Consumer bằng Redis Key (setIfAbsent với TTL 24h)
        RBucket<String> idempotencyBucket = redissonClient.getBucket(idempotencyKey);
        boolean isFirstExecution = idempotencyBucket.setIfAbsent("PROCESSING", IDEMPOTENCY_TTL);

        if (!isFirstExecution) {
            String currentStatus = idempotencyBucket.get();
            log.warn("[SHIPPING-SERVICE][IDEMPOTENT HIT] Yêu cầu hủy đơn [{}] đã được tiếp nhận từ trước với trạng thái [{}] trong Redis. Bỏ qua lặp.",
                    command.getOrderNumber(), currentStatus);
            return;
        }

        try {
            // 2. Tìm thông tin vận đơn hiện tại trong DB (nếu chưa có thì tạo mới theo đơn hàng)
            Optional<Shipment> shipmentOpt = shipmentRepository.findByOrderId(command.getOrderId());
            Shipment shipment;

            String carrier = (command.getCarrier() != null && !command.getCarrier().isBlank())
                    ? command.getCarrier()
                    : "AHAMOVE";
            String trackingCode = (command.getTrackingCode() != null && !command.getTrackingCode().isBlank())
                    ? command.getTrackingCode()
                    : "AHA-" + command.getOrderNumber().replace("XC-", "");

            if (shipmentOpt.isPresent()) {
                shipment = shipmentOpt.get();
            } else {
                shipment = Shipment.builder()
                        .orderId(command.getOrderId())
                        .orderNumber(command.getOrderNumber())
                        .pharmacyHubId(command.getPharmacyHubId())
                        .carrier(carrier)
                        .trackingCode(trackingCode)
                        .status("ASSIGNED_SHIPPER")
                        .build();
            }

            // 3. Gọi API hủy vận đơn sang đối tác 3PL (Ahamove / GHTK)
            ThirdPartyLogisticsClient.Cancel3PLResponse cancelResult = thirdPartyLogisticsClient.cancelShipment(
                    shipment.getCarrier(),
                    shipment.getTrackingCode(),
                    command.getReason()
            );

            log.info("[SHIPPING-SERVICE] Kết quả hủy từ 3PL [{}]: success={}, message={}",
                    shipment.getCarrier(), cancelResult.isSuccess(), cancelResult.getMessage());

            // 4. Cập nhật trạng thái vận đơn thành CANCELLED trong Database PostgreSQL
            shipment.setStatus("CANCELLED");
            shipment.setCancellationReason(command.getReason());
            shipmentRepository.save(shipment);

            // 5. Cập nhật Redis Idempotency status thành COMPLETED
            idempotencyBucket.set("COMPLETED", IDEMPOTENCY_TTL);

            // 6. Xây dựng Response Event SHIPMENT_CANCELLED
            ShipmentCancelledEvent responseEvent = ShipmentCancelledEvent.builder()
                    .eventId(UUID.randomUUID())
                    .sagaId(command.getSagaId())
                    .orderId(command.getOrderId())
                    .orderNumber(command.getOrderNumber())
                    .pharmacyHubId(command.getPharmacyHubId())
                    .trackingCode(shipment.getTrackingCode())
                    .carrier(shipment.getCarrier())
                    .status("SHIPMENT_CANCELLED")
                    .cancelReason(command.getReason())
                    .cancelledAt(Instant.now())
                    .items(command.getItems() != null ? command.getItems().stream()
                            .map(i -> ShipmentCancelledEvent.ShippingItemPayload.builder()
                                    .sku(i.getSku())
                                    .productName(i.getProductName())
                                    .quantity(i.getQuantity())
                                    .build())
                            .toList() : java.util.Collections.emptyList())
                    .build();

            String payloadJson = objectMapper.writeValueAsString(responseEvent);

            // 7. Transactional Outbox Pattern: Lưu Outbox Event vào PostgreSQL trong cùng Transaction
            ShippingOutboxEvent outboxEvent = ShippingOutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("SHIPPING")
                    .aggregateId(shipment.getId() != null ? shipment.getId().toString() : command.getOrderId().toString())
                    .eventType("SHIPMENT_CANCELLED")
                    .topic(orderSagaResponsesTopic)
                    .partitionKey(command.getOrderNumber())
                    .payload(payloadJson)
                    .status("PUBLISHED")
                    .retryCount(0)
                    .createdAt(Instant.now())
                    .publishedAt(Instant.now())
                    .build();
            shippingOutboxEventRepository.save(outboxEvent);

            // 8. Bắn Response Event về topic 'order-saga-responses' cho Order Saga Orchestrator
            kafkaTemplate.send(orderSagaResponsesTopic, command.getOrderNumber(), payloadJson);
            if (!orderSagaResponsesTopic.equals(shippingCancellationTopic)) {
                kafkaTemplate.send(shippingCancellationTopic, command.getOrderNumber(), payloadJson);
            }

            log.info("[SHIPPING-SERVICE][BƯỚC 2] HOÀN TẤT! Đã gửi SHIPMENT_CANCELLED về topic '{}' cho đơn [{}]",
                    orderSagaResponsesTopic, command.getOrderNumber());

        } catch (Exception ex) {
            log.error("[SHIPPING-SERVICE] Lỗi trong tiến trình hủy vận đơn cho đơn [{}]: {}",
                    command.getOrderNumber(), ex.getMessage(), ex);
            idempotencyBucket.delete();
            throw new RuntimeException("Thất bại khi xử lý CancelShipmentCommand", ex);
        }
    }

    /**
     * Tương thích ngược: Xử lý event từ Choreography Flow (OrderCancelRequestedEvent)
     */
    @Transactional
    public void processCancellation(OrderCancelRequestedEvent event) {
        CancelShipmentCommand command = CancelShipmentCommand.builder()
                .commandId(UUID.randomUUID())
                .sagaId(event.getSagaId())
                .orderId(event.getOrderId())
                .orderNumber(event.getOrderNumber())
                .pharmacyHubId(event.getPharmacyHubId())
                .carrier("AHAMOVE")
                .reason(event.getCancelReason())
                .cancelledBy(event.getRequestedBy())
                .createdAt(Instant.now())
                .items(event.getItems() != null ? event.getItems().stream()
                        .map(i -> CancelShipmentCommand.CancelItemPayload.builder()
                                .sku(i.getSku())
                                .productName(i.getProductName())
                                .quantity(i.getQuantity())
                                .build())
                        .toList() : java.util.Collections.emptyList())
                .build();
        processCancelShipmentCommand(command);
    }

    /**
     * BƯỚC 2 (Task 2): Lắng nghe FULFILLMENT_FAILED từ fulfillment-events.
     */
    @Transactional
    public void processFulfillmentFailure(FulfillmentFailedEvent event) {
        String idempotencyKey = IDEMPOTENCY_PREFIX + "fulfillment-failed:" + event.getOrderId();
        log.info("BƯỚC 2 (Shipping Service): Tiếp nhận FULFILLMENT_FAILED cho đơn [{}], Hub [{}], SagaId [{}]",
                event.getOrderNumber(), event.getPharmacyHubId(), event.getSagaId());

        RBucket<String> idempotencyBucket = redissonClient.getBucket(idempotencyKey);
        boolean isFirstExecution = idempotencyBucket.setIfAbsent("PROCESSING", IDEMPOTENCY_TTL);

        if (!isFirstExecution) {
            String currentStatus = idempotencyBucket.get();
            log.warn("IDEMPOTENT HIT: Yêu cầu hủy giao hàng do lỗi đóng gói cho đơn [{}] đã xử lý (Trạng thái: [{}]). Bỏ qua lặp.",
                    event.getOrderNumber(), currentStatus);
            return;
        }

        try {
            Optional<Shipment> shipmentOpt = shipmentRepository.findByOrderId(event.getOrderId());
            Shipment shipment;

            if (shipmentOpt.isPresent()) {
                shipment = shipmentOpt.get();
            } else {
                shipment = Shipment.builder()
                        .orderId(event.getOrderId())
                        .orderNumber(event.getOrderNumber())
                        .pharmacyHubId(event.getPharmacyHubId())
                        .carrier("AHAMOVE")
                        .trackingCode("AHA-" + event.getOrderNumber().replace("XC-", ""))
                        .status("ASSIGNED_SHIPPER")
                        .build();
            }

            String cancelNote = "Hủy cuốc xe do dược sĩ hủy đóng gói (Thuốc hỏng/hết hàng): " + event.getFailedReason();
            ThirdPartyLogisticsClient.Cancel3PLResponse cancelResult = thirdPartyLogisticsClient.cancelShipment(
                    shipment.getCarrier(),
                    shipment.getTrackingCode(),
                    cancelNote
            );

            log.info("Kết quả hủy Ahamove khi kho hủy đóng gói [{}]: success={}, message={}",
                    shipment.getTrackingCode(), cancelResult.isSuccess(), cancelResult.getMessage());

            shipment.setStatus("CANCELLED");
            shipment.setCancellationReason("FULFILLMENT_FAILED: " + event.getFailedReason());
            shipmentRepository.save(shipment);

            idempotencyBucket.set("COMPLETED", IDEMPOTENCY_TTL);

            ShipmentCancelledEvent cancelledEvent = ShipmentCancelledEvent.builder()
                    .eventId(UUID.randomUUID())
                    .sagaId(event.getSagaId())
                    .orderId(event.getOrderId())
                    .orderNumber(event.getOrderNumber())
                    .pharmacyHubId(event.getPharmacyHubId())
                    .trackingCode(shipment.getTrackingCode())
                    .carrier(shipment.getCarrier())
                    .status("SHIPMENT_CANCELLED")
                    .cancelReason("FULFILLMENT_FAILED: " + event.getFailedReason())
                    .cancelledAt(Instant.now())
                    .items(event.getItems() != null ? event.getItems().stream()
                            .map(i -> ShipmentCancelledEvent.ShippingItemPayload.builder()
                                    .sku(i.getSku())
                                    .productName(i.getProductName())
                                    .quantity(i.getQuantity())
                                    .build())
                            .toList() : java.util.Collections.emptyList())
                    .build();

            String payloadJson = objectMapper.writeValueAsString(cancelledEvent);

            kafkaTemplate.send(shippingCancellationTopic, event.getOrderNumber(), payloadJson);
            if (!shippingCancellationTopic.equals(shippingEventsTopic)) {
                kafkaTemplate.send(shippingEventsTopic, event.getOrderNumber(), payloadJson);
            }
            kafkaTemplate.send(orderSagaResponsesTopic, event.getOrderNumber(), payloadJson);

            log.info("BƯỚC 2 (Shipping Service): ĐÃ BẮN EVENT SHIPMENT_CANCELLED thành công cho đơn [{}]",
                    event.getOrderNumber());

        } catch (Exception ex) {
            log.error("Lỗi khi xử lý hủy giao hàng do đóng gói thất bại cho đơn [{}]: {}", event.getOrderNumber(), ex.getMessage(), ex);
            idempotencyBucket.delete();
            throw new RuntimeException("Thất bại khi hủy vận đơn Ahamove do lỗi đóng gói kho", ex);
        }
    }
}
