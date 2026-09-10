package com.xcare.fulfillment.pack.service;

import com.xcare.fulfillment.pack.domain.FulfillmentOutboxEvent;
import com.xcare.fulfillment.pack.domain.OutboxStatus;
import com.xcare.fulfillment.pack.repository.FulfillmentOutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Worker nền quét bảng fulfillment_outbox với SKIP LOCKED và đẩy tin sang Kafka.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FulfillmentOutboxPublisherService {

    private final FulfillmentOutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${xcare.outbox.publisher.batch-size:50}")
    private int batchSize;

    @Value("${xcare.outbox.publisher.max-retry:5}")
    private int maxRetry;

    @Scheduled(fixedDelayString = "${xcare.outbox.publisher.fixed-delay-ms:2000}")
    public void publishPendingEvents() {
        List<FulfillmentOutboxEvent> pendingEvents = fetchPendingEvents();

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Fulfillment Outbox Publisher: Đang xử lý [{}] sự kiện outbox pending", pendingEvents.size());

        for (FulfillmentOutboxEvent event : pendingEvents) {
            publishSingleEvent(event);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<FulfillmentOutboxEvent> fetchPendingEvents() {
        return outboxEventRepository.findPendingEventsWithSkipLocked(batchSize, maxRetry);
    }

    private void publishSingleEvent(FulfillmentOutboxEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                event.getTopic(),
                event.getPartitionKey(),
                event.getPayload()
        );

        record.headers().add("X-Correlation-Id", event.getId().toString().getBytes());
        record.headers().add("X-Event-Type", event.getEventType().getBytes());
        record.headers().add("X-Aggregate-Type", event.getAggregateType().getBytes());

        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("BƯỚC 1 (Fulfillment Publisher): Đã bắn thành công Outbox Event [{}] vào topic [{}] offset [{}]",
                        event.getId(), event.getTopic(), result.getRecordMetadata().offset());
                markAsPublished(event.getId());
            } else {
                log.error("Thất bại khi gửi outbox event [{}] sang Kafka: {}", event.getId(), ex.getMessage());
                incrementRetry(event.getId(), ex.getMessage());
            }
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markAsPublished(java.util.UUID eventId) {
        outboxEventRepository.findById(eventId).ifPresent(e -> {
            e.setStatus(OutboxStatus.PUBLISHED);
            e.setPublishedAt(Instant.now());
            outboxEventRepository.save(e);
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void incrementRetry(java.util.UUID eventId, String error) {
        outboxEventRepository.findById(eventId).ifPresent(e -> {
            e.setRetryCount(e.getRetryCount() + 1);
            e.setErrorMessage(error != null && error.length() > 950 ? error.substring(0, 950) : error);
            if (e.getRetryCount() >= maxRetry) {
                e.setStatus(OutboxStatus.FAILED);
            }
            outboxEventRepository.save(e);
        });
    }
}
