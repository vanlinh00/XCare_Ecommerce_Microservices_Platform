package com.xcare.shipping.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.shipping.event.FulfillmentFailedEvent;
import com.xcare.shipping.service.ShippingCancellationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka Consumer lắng nghe sự kiện FULFILLMENT_FAILED từ topic 'fulfillment-events'
 * để kích hoạt hủy vận chuyển 3PL (Ahamove).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FulfillmentFailedKafkaListener {

    private final ShippingCancellationService shippingCancellationService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${xcare.topics.fulfillment-events:fulfillment-events}",
            groupId = "${spring.kafka.consumer.group-id:xcare-shipping-cancellation-group}"
    )
    public void onFulfillmentFailed(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("Shipping Service nhận tin nhắn FULFILLMENT_FAILED từ topic [{}], partition [{}], offset [{}]",
                record.topic(), record.partition(), record.offset());

        try {
            FulfillmentFailedEvent event = objectMapper.readValue(record.value(), FulfillmentFailedEvent.class);
            log.info("Deserialize thành công FulfillmentFailedEvent cho đơn [{}], SagaId [{}]",
                    event.getOrderNumber(), event.getSagaId());

            shippingCancellationService.processFulfillmentFailure(event);

            if (ack != null) {
                ack.acknowledge();
            }
        } catch (Exception e) {
            log.error("Lỗi khi xử lý tin nhắn FULFILLMENT_FAILED từ Kafka: {}", e.getMessage(), e);
            // Trong production có thể commit hoặc đẩy sang DLT (Dead Letter Topic)
            if (ack != null) {
                ack.acknowledge();
            }
        }
    }
}
