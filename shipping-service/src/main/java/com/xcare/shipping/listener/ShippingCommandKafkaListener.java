package com.xcare.shipping.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xcare.shipping.event.CancelShipmentCommand;
import com.xcare.shipping.service.ShippingCancellationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * BƯỚC 2 trong Saga Orchestration Flow:
 * Kafka Consumer lắng nghe Command hủy vận đơn từ Order Saga Orchestrator (:8081).
 * Lắng nghe Topic: 'shipping-commands'
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShippingCommandKafkaListener {

    private final ShippingCancellationService shippingCancellationService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${xcare.topics.shipping-commands:shipping-commands}",
            groupId = "xcare-shipping-commands-group"
    )
    public void onCancelShipmentCommand(ConsumerRecord<String, String> record) {
        log.info("[SHIPPING-COMMAND-LISTENER][BƯỚC 2] Nhận CancelShipmentCommand từ topic [{}] key [{}] offset [{}]",
                record.topic(), record.key(), record.offset());

        try {
            CancelShipmentCommand command = objectMapper.readValue(record.value(), CancelShipmentCommand.class);
            log.info("[SHIPPING-COMMAND-LISTENER] Đang chuyển giao lệnh hủy đơn [{}] - Saga [{}] cho ShippingCancellationService",
                    command.getOrderNumber(), command.getSagaId());

            shippingCancellationService.processCancelShipmentCommand(command);

        } catch (Exception e) {
            log.error("[SHIPPING-COMMAND-LISTENER] Lỗi khi giải mã hoặc xử lý CancelShipmentCommand: {}",
                    record.value(), e);
        }
    }
}
