package com.xcare.shipping.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.shipping.client.ThirdPartyLogisticsClient;
import com.xcare.shipping.domain.entity.Shipment;
import com.xcare.shipping.event.FulfillmentFailedEvent;
import com.xcare.shipping.event.OrderCancelRequestedEvent;
import com.xcare.shipping.event.ShipmentCancelledEvent;
import com.xcare.shipping.repository.ShipmentRepository;
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
 * Xử lý bước 2 trong Saga Rollback: Hủy chuyến vận chuyển 3PL (Ahamove / GHTK).
 * Đảm bảo tính Idempotent Consumer bằng Redis Key trước khi gửi event SHIPMENT_CANCELLED sang Kafka.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShippingCancellationService {

    private final ShipmentRepository shipmentRepository;
    private final ThirdPartyLogisticsClient thirdPartyLogisticsClient;
    private final RedissonClient redissonClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${xcare.topics.shipping-cancellation:shipping-cancellation-events}")
    private String shippingCancellationTopic;

    @Value("${xcare.topics.shipping-events:shipping-events}")
    private String shippingEventsTopic;

    private static final String IDEMPOTENCY_PREFIX = "xcare:idempotency:shipping:cancel:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    @Transactional
    public void processCancellation(OrderCancelRequestedEvent event) {
        String idempotencyKey = IDEMPOTENCY_PREFIX + event.getOrderId();
        log.info("BƯỚC 2 (Shipping Service): Bắt đầu xử lý hủy vận chuyển cho đơn [{}], SagaId [{}]",
                event.getOrderNumber(), event.getSagaId());

        // 1. Kiểm tra tính Idempotency bằng Redis Key
        RBucket<String> idempotencyBucket = redissonClient.getBucket(idempotencyKey);
        boolean isFirstExecution = idempotencyBucket.setIfAbsent("PROCESSING", IDEMPOTENCY_TTL);

        if (!isFirstExecution) {
            String currentStatus = idempotencyBucket.get();
            log.warn("IDEMPOTENT HIT: Yêu cầu hủy đơn [{}] đã được tiếp nhận từ trước với trạng thái [{}] trong Redis. Bỏ qua để chống trùng lặp.",
                    event.getOrderNumber(), currentStatus);
            return;
        }

        try {
            // 2. Tìm thông tin vận đơn hiện tại trong DB (nếu chưa có thì khởi tạo bản ghi vận đơn Ahamove hỏa tốc)
            Optional<Shipment> shipmentOpt = shipmentRepository.findByOrderId(event.getOrderId());
            Shipment shipment;

            if (shipmentOpt.isPresent()) {
                shipment = shipmentOpt.get();
            } else {
                // Giả lập kịch bản: Đơn hàng vừa gán cho tài xế Ahamove hỏa tốc qua ứng dụng
                shipment = Shipment.builder()
                        .orderId(event.getOrderId())
                        .orderNumber(event.getOrderNumber())
                        .pharmacyHubId(event.getPharmacyHubId())
                        .carrier("AHAMOVE")
                        .trackingCode("AHA-" + event.getOrderNumber().replace("XC-", ""))
                        .status("ASSIGNED_SHIPPER")
                        .build();
            }

            // 3. Gọi API hủy vận đơn sang đối tác 3PL (Ahamove/GHTK)
            ThirdPartyLogisticsClient.Cancel3PLResponse cancelResult = thirdPartyLogisticsClient.cancelShipment(
                    shipment.getCarrier(),
                    shipment.getTrackingCode(),
                    event.getCancelReason()
            );

            log.info("Kết quả hủy từ 3PL [{}]: success={}, message={}",
                    shipment.getCarrier(), cancelResult.isSuccess(), cancelResult.getMessage());

            // 4. Cập nhật trạng thái vận đơn thành CANCELLED trong Database
            shipment.setStatus("CANCELLED");
            shipment.setCancellationReason(event.getCancelReason());
            shipmentRepository.save(shipment);

            // 5. Cập nhật Redis Idempotency status thành COMPLETED
            idempotencyBucket.set("COMPLETED", IDEMPOTENCY_TTL);

            // 6. Bắn Kafka Event SHIPMENT_CANCELLED sang topic 'shipping-cancellation-events'
            ShipmentCancelledEvent cancelledEvent = ShipmentCancelledEvent.builder()
                    .eventId(UUID.randomUUID())
                    .sagaId(event.getSagaId())
                    .orderId(event.getOrderId())
                    .orderNumber(event.getOrderNumber())
                    .pharmacyHubId(event.getPharmacyHubId())
                    .trackingCode(shipment.getTrackingCode())
                    .carrier(shipment.getCarrier())
                    .status("SHIPMENT_CANCELLED")
                    .cancelReason(event.getCancelReason())
                    .cancelledAt(Instant.now())
                    .items(event.getItems().stream()
                            .map(i -> ShipmentCancelledEvent.ShippingItemPayload.builder()
                                    .sku(i.getSku())
                                    .productName(i.getProductName())
                                    .quantity(i.getQuantity())
                                    .build())
                            .toList())
                    .build();

            String payloadJson = objectMapper.writeValueAsString(cancelledEvent);

            kafkaTemplate.send(shippingCancellationTopic, event.getOrderNumber(), payloadJson)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("BƯỚC 2 (Shipping Service): ĐÃ BẮN EVENT SHIPMENT_CANCELLED thành công cho đơn [{}] vào topic [{}]",
                                    event.getOrderNumber(), shippingCancellationTopic);
                        } else {
                            log.error("Lỗi khi bắn event SHIPMENT_CANCELLED vào Kafka: {}", ex.getMessage(), ex);
                        }
                    });

        } catch (Exception ex) {
            log.error("Lỗi ngoại lệ trong tiến trình hủy vận đơn cho đơn [{}]: {}", event.getOrderNumber(), ex.getMessage(), ex);
            // Xóa key Redis để cho phép retry nếu xảy ra lỗi hệ thống
            idempotencyBucket.delete();
            throw new RuntimeException("Thất bại khi hủy vận đơn 3PL", ex);
        }
    }

    /**
     * BƯỚC 2 (Task 2): Lắng nghe FULFILLMENT_FAILED từ fulfillment-events.
     * Gọi API Ahamove để hủy cuốc xe shipper đã book, cập nhật trạng thái CANCELLED,
     * và bắn sự kiện SHIPMENT_CANCELLED sang Kafka.
     */
    @Transactional
    public void processFulfillmentFailure(FulfillmentFailedEvent event) {
        String idempotencyKey = IDEMPOTENCY_PREFIX + "fulfillment-failed:" + event.getOrderId();
        log.info("BƯỚC 2 (Shipping Service): Tiếp nhận FULFILLMENT_FAILED cho đơn [{}], Hub [{}], SagaId [{}]",
                event.getOrderNumber(), event.getPharmacyHubId(), event.getSagaId());

        // 1. Kiểm tra tính Idempotency bằng Redis Key
        RBucket<String> idempotencyBucket = redissonClient.getBucket(idempotencyKey);
        boolean isFirstExecution = idempotencyBucket.setIfAbsent("PROCESSING", IDEMPOTENCY_TTL);

        if (!isFirstExecution) {
            String currentStatus = idempotencyBucket.get();
            log.warn("IDEMPOTENT HIT: Yêu cầu hủy giao hàng do lỗi đóng gói cho đơn [{}] đã xử lý (Trạng thái: [{}]). Bỏ qua lặp.",
                    event.getOrderNumber(), currentStatus);
            return;
        }

        try {
            // 2. Tìm hoặc tạo bản ghi Shipment
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

            // 3. Gọi API hủy vận đơn Ahamove 3PL
            String cancelNote = "Hủy cuốc xe do dược sĩ hủy đóng gói (Thuốc hỏng/hết hàng): " + event.getFailedReason();
            ThirdPartyLogisticsClient.Cancel3PLResponse cancelResult = thirdPartyLogisticsClient.cancelShipment(
                    shipment.getCarrier(),
                    shipment.getTrackingCode(),
                    cancelNote
            );

            log.info("Kết quả hủy Ahamove khi kho hủy đóng gói [{}]: success={}, message={}",
                    shipment.getTrackingCode(), cancelResult.isSuccess(), cancelResult.getMessage());

            // 4. Cập nhật trạng thái Shipment
            shipment.setStatus("CANCELLED");
            shipment.setCancellationReason("FULFILLMENT_FAILED: " + event.getFailedReason());
            shipmentRepository.save(shipment);

            // 5. Cập nhật Redis Idempotency status thành COMPLETED
            idempotencyBucket.set("COMPLETED", IDEMPOTENCY_TTL);

            // 6. Bắn Kafka Event SHIPMENT_CANCELLED
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

            // Bắn vào cả topic shipping-cancellation-events và shipping-events
            kafkaTemplate.send(shippingCancellationTopic, event.getOrderNumber(), payloadJson);
            if (!shippingCancellationTopic.equals(shippingEventsTopic)) {
                kafkaTemplate.send(shippingEventsTopic, event.getOrderNumber(), payloadJson);
            }

            log.info("BƯỚC 2 (Shipping Service): ĐÃ BẮN EVENT SHIPMENT_CANCELLED thành công cho đơn [{}] vào topics [{}, {}]",
                    event.getOrderNumber(), shippingCancellationTopic, shippingEventsTopic);

        } catch (Exception ex) {
            log.error("Lỗi khi xử lý hủy giao hàng do đóng gói thất bại cho đơn [{}]: {}", event.getOrderNumber(), ex.getMessage(), ex);
            idempotencyBucket.delete();
            throw new RuntimeException("Thất bại khi hủy vận đơn Ahamove do lỗi đóng gói kho", ex);
        }
    }
}
