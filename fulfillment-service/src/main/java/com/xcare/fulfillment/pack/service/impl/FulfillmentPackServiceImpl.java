package com.xcare.fulfillment.pack.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.fulfillment.pack.domain.FulfillmentOutboxEvent;
import com.xcare.fulfillment.pack.domain.OutboxStatus;
import com.xcare.fulfillment.pack.dto.CancelPackRequest;
import com.xcare.fulfillment.pack.dto.CancelPackResponse;
import com.xcare.fulfillment.pack.event.FulfillmentFailedEvent;
import com.xcare.fulfillment.pack.repository.FulfillmentOutboxEventRepository;
import com.xcare.fulfillment.pack.service.FulfillmentPackService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class FulfillmentPackServiceImpl implements FulfillmentPackService {

    private final FulfillmentOutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Value("${xcare.topics.fulfillment-events:fulfillment-events}")
    private String fulfillmentEventsTopic;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CancelPackResponse cancelPackaging(CancelPackRequest request) {
        log.info("BƯỚC 1 (Fulfillment Service): Dược sĩ [{}] yêu cầu HỦY ĐÓNG GÓI cho đơn [{}], Hub [{}]. Lý do: [{}]",
                request.getPharmacistId(), request.getOrderNumber(), request.getPharmacyHubId(), request.getReason());

        if (request.getOrderId() == null || request.getOrderNumber() == null) {
            throw new IllegalArgumentException("OrderId và OrderNumber không được để trống khi hủy đóng gói");
        }

        // 1. Sinh Saga Correlation ID duy nhất
        String sagaId = "SAGA-FULFILLMENT-FAIL-" + request.getOrderNumber() + "-" + UUID.randomUUID().toString().substring(0, 8);

        // 2. Chuyển đổi line items
        List<FulfillmentFailedEvent.FailedItemPayload> eventItems = request.getItems() != null
                ? request.getItems().stream()
                .map(i -> FulfillmentFailedEvent.FailedItemPayload.builder()
                        .sku(i.getSku())
                        .productName(i.getProductName())
                        .quantity(i.getQuantity())
                        .damaged(i.isDamaged())
                        .defectNote(i.getDefectNote())
                        .build())
                .toList()
                : Collections.emptyList();

        // 3. Khởi tạo Event FULFILLMENT_FAILED
        FulfillmentFailedEvent event = FulfillmentFailedEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(request.getOrderId())
                .orderNumber(request.getOrderNumber())
                .pharmacyHubId(request.getPharmacyHubId())
                .pharmacistId(request.getPharmacistId() != null ? request.getPharmacistId() : "PHARMACIST_HUB")
                .failedReason(request.getReason() != null ? request.getReason() : "Thuốc bị hỏng/hết hàng trên kệ kho")
                .status("FULFILLMENT_FAILED")
                .failedAt(Instant.now())
                .items(eventItems)
                .build();

        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.error("Lỗi khi serialize FulfillmentFailedEvent", e);
            throw new IllegalStateException("Không thể serialize sự kiện FULFILLMENT_FAILED", e);
        }

        // 4. Lưu bản ghi Outbox vào PostgreSQL trong CÙNG ACID Transaction
        FulfillmentOutboxEvent outboxEvent = FulfillmentOutboxEvent.builder()
                .aggregateType("FULFILLMENT_SAGA")
                .aggregateId(request.getOrderId().toString())
                .eventType(FulfillmentFailedEvent.class.getSimpleName())
                .topic(fulfillmentEventsTopic)
                .partitionKey(request.getOrderNumber())
                .payload(payloadJson)
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .build();

        outboxEventRepository.save(outboxEvent);

        log.info("BƯỚC 1 (Fulfillment Service): Đã lưu Outbox Event [{}] thành công vào topic [{}] với partitionKey [{}]",
                outboxEvent.getId(), fulfillmentEventsTopic, request.getOrderNumber());

        return CancelPackResponse.builder()
                .orderId(request.getOrderId())
                .orderNumber(request.getOrderNumber())
                .sagaId(sagaId)
                .status("FULFILLMENT_FAILED")
                .message("Đã ghi nhận hủy đóng gói do sự cố hàng hóa. Hệ thống đang kích hoạt Saga Rollback tới Đơn vị Vận chuyển và Quản lý Đơn.")
                .cancelledAt(Instant.now())
                .build();
    }
}
