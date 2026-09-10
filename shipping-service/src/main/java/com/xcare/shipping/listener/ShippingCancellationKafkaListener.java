package com.xcare.shipping.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.shipping.event.OrderCancelRequestedEvent;
import com.xcare.shipping.service.ShippingCancellationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka Consumer lắng nghe yêu cầu hủy đơn thuốc từ Order Service (Bước 2 trong Saga).
 * Lắng nghe Topic: 'order-cancellation-events'
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShippingCancellationKafkaListener {

    private final ShippingCancellationService shippingCancellationService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${xcare.topics.order-cancellation:order-cancellation-events}",
            groupId = "xcare-shipping-cancellation-group"
    )
    public void onOrderCancelRequested(ConsumerRecord<String, String> record) {
        log.info("BƯỚC 2 (Shipping Kafka Listener): Nhận yêu cầu hủy từ topic [{}] key [{}]",
                record.topic(), record.key());

        try {
            OrderCancelRequestedEvent event = objectMapper.readValue(record.value(), OrderCancelRequestedEvent.class);
            shippingCancellationService.processCancellation(event);
        } catch (Exception e) {
            log.error("Lỗi khi xử lý message từ Kafka: {}", record.value(), e);
        }
    }
}
