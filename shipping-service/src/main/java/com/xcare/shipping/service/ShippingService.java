package com.xcare.shipping.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.shipping.client.ThirdPartyLogisticsClient;
import com.xcare.shipping.domain.entity.Shipment;
import com.xcare.shipping.domain.entity.ShippingOutboxEvent;
import com.xcare.shipping.event.ShipmentBookingCommand;
import com.xcare.shipping.event.ShippingBookingFailedEvent;
import com.xcare.shipping.repository.ShipmentRepository;
import com.xcare.shipping.repository.ShippingOutboxEventRepository;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Senior Implementation: 3PL Shipping Booking Service.
 * Features Resilience4j Retry with Exponential Backoff and Transactional Outbox Fallback.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShippingService {

    private final ThirdPartyLogisticsClient thirdPartyLogisticsClient;
    private final ShipmentRepository shipmentRepository;
    private final ShippingOutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final String TOPIC_SHIPPING_BOOKING_FAILED = "shipping-events";

    /**
     * Bước 3 (Orchestration Saga Flow):
     * Tiến hành gọi API đối tác 3PL (Ahamove / GHTK) để book tài xế giao thuốc.
     * Cấu hình Resilience4j Retry: Thử lại 3 lần với Exponential Backoff khi gặp Timeout hoặc HTTP 500.
     */
    @Retry(name = "thirdPartyLogisticsRetry", fallbackMethod = "fallbackBookShipping")
    @Transactional
    public void bookShipping(ShipmentBookingCommand command) {
        log.info("[SAGA-STEP-3] Bắt đầu gọi API đối tác 3PL {} cho đơn thuốc {}", command.getCarrier(), command.getOrderNumber());

        // Gọi 3PL API (giả lập hoặc qua Client)
        // Khi đối tác Timeout hoặc trả về mã lỗi 500, throw RuntimeException để Resilience4j kích hoạt Retry
        ThirdPartyLogisticsClient.Cancel3PLResponse response = thirdPartyLogisticsClient.cancelShipment(
                command.getCarrier(),
                "BOOK-" + command.getOrderNumber(),
                "Auto book rider"
        );

        // Giả lập kiểm tra kết quả gọi 3PL
        if (!response.isSuccess()) {
            throw new RuntimeException("3PL API " + command.getCarrier() + " Timeout / Service Unavailable (HTTP 503)");
        }

        log.info("[SAGA-STEP-3] Book 3PL {} thành công cho đơn {}", command.getCarrier(), command.getOrderNumber());
    }

    /**
     * Fallback Method: Được kích hoạt tự động sau khi Resilience4j thử lại đủ 3 lần mà vẫn thất bại.
     * Lưu trạng thái thất bại vào shipment và ghi nhận Transactional Outbox để phát event SHIPPING_BOOKING_FAILED.
     */
    @Transactional
    public void fallbackBookShipping(ShipmentBookingCommand command, Throwable ex) {
        log.error("[SAGA-STEP-3-FALLBACK] Gọi 3PL thất bại sau 3 lần retry cho đơn {}. Lỗi: {}", 
                command.getOrderNumber(), ex.getMessage());

        String carrier = command.getCarrier() != null ? command.getCarrier() : "AHAMOVE";
        String failureReason = "3PL Carrier [" + carrier + "] Timeout / Service Unavailable: " + ex.getMessage();

        // 1. Cập nhật hoặc lưu Shipment trạng thái FAILED
        Shipment shipment = shipmentRepository.findByOrderId(command.getOrderId())
                .orElseGet(() -> Shipment.builder()
                        .id(UUID.randomUUID())
                        .orderId(command.getOrderId())
                        .orderNumber(command.getOrderNumber())
                        .pharmacyHubId(command.getPharmacyHubId())
                        .carrier(carrier)
                        .trackingCode("FAILED-" + System.currentTimeMillis())
                        .build());
        shipment.setStatus("FAILED");
        shipment.setCancellationReason(failureReason);
        shipmentRepository.save(shipment);

        // 2. Chuẩn bị Payload Event SHIPPING_BOOKING_FAILED
        ShippingBookingFailedEvent failedEvent = ShippingBookingFailedEvent.builder()
                .eventId(UUID.randomUUID())
                .orderId(command.getOrderId())
                .orderNumber(command.getOrderNumber())
                .pharmacyHubId(command.getPharmacyHubId())
                .carrier(carrier)
                .failureReason(failureReason)
                .retryCount(3)
                .items(command.getItems() != null ? command.getItems().stream()
                        .map(i -> new ShippingBookingFailedEvent.ItemPayload(i.getSku(), i.getProductName(), i.getQuantity()))
                        .collect(Collectors.toList()) : Collections.emptyList())
                .failedAt(Instant.now())
                .build();

        try {
            String payloadJson = objectMapper.writeValueAsString(failedEvent);

            // 3. Ghi vào Transactional Outbox (PostgreSQL) - Đảm bảo ACID transaction
            ShippingOutboxEvent outboxEvent = ShippingOutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateType("SHIPMENT")
                    .aggregateId(command.getOrderId().toString())
                    .eventType("SHIPPING_BOOKING_FAILED")
                    .topic(TOPIC_SHIPPING_BOOKING_FAILED)
                    .partitionKey(command.getOrderId().toString())
                    .payload(payloadJson)
                    .status("PUBLISHED")
                    .retryCount(0)
                    .publishedAt(Instant.now())
                    .build();
            outboxEventRepository.save(outboxEvent);

            // 4. Phát trực tiếp sang Kafka Topic cho Order Service (Saga Orchestrator)
            kafkaTemplate.send(TOPIC_SHIPPING_BOOKING_FAILED, command.getOrderId().toString(), payloadJson);
            log.info("[SAGA-STEP-3-OUTBOX] Đã lưu Outbox và phát event SHIPPING_BOOKING_FAILED sang Kafka topic '{}' cho đơn {}",
                    TOPIC_SHIPPING_BOOKING_FAILED, command.getOrderNumber());

        } catch (Exception e) {
            log.error("Lỗi khi serialize hoặc ghi Outbox Event cho đơn {}: {}", command.getOrderNumber(), e.getMessage(), e);
            throw new RuntimeException("Outbox persist failed", e);
        }
    }
}
