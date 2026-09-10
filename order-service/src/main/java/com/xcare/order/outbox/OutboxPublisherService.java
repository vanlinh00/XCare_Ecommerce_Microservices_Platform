package com.xcare.order.outbox;

import com.xcare.order.domain.entity.OutboxEvent;
import com.xcare.order.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Outbox Publisher Service
 *
 * Implements the Transactional Outbox background polling worker.
 * Periodically polls the 'outbox_events' table using PostgreSQL 'SKIP LOCKED'
 * and delivers messages to Apache Kafka 3.7.0 with at-least-once delivery guarantees.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxPublisherService {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${xcare.outbox.publisher.batch-size:50}")
    private int batchSize;

    @Value("${xcare.outbox.publisher.max-retry:5}")
    private int maxRetry;

    /**
     * Scheduled background poller.
     * Executes every 2000ms (configurable).
     */
    @Scheduled(fixedDelayString = "${xcare.outbox.publisher.fixed-delay-ms:2000}")
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = fetchPendingEvents();

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Processing [{}] pending outbox events for Kafka publication", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            publishSingleEvent(event);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<OutboxEvent> fetchPendingEvents() {
        return outboxEventRepository.findPendingEventsWithSkipLocked(batchSize, maxRetry);
    }

    private void publishSingleEvent(OutboxEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                event.getTopic(),
                event.getPartitionKey(),
                event.getPayload()
        );

        // Add standard XCare platform correlation headers
        record.headers().add("X-Correlation-Id", event.getId().toString().getBytes());
        record.headers().add("X-Event-Type", event.getEventType().getBytes());
        record.headers().add("X-Aggregate-Type", event.getAggregateType().getBytes());
        record.headers().add("X-Aggregate-Id", event.getAggregateId().getBytes());

        log.debug("Publishing outbox event [{}] to Kafka topic [{}]", event.getId(), event.getTopic());

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                long offset = result.getRecordMetadata().offset();
                int partition = result.getRecordMetadata().partition();
                log.info("Successfully published outbox event [{}] to Kafka [topic={}, partition={}, offset={}]",
                        event.getId(), event.getTopic(), partition, offset);
                markEventPublished(event);
            } else {
                log.error("Failed to publish outbox event [{}] to Kafka: {}", event.getId(), ex.getMessage(), ex);
                handlePublishFailure(event, ex.getMessage());
            }
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markEventPublished(OutboxEvent event) {
        outboxEventRepository.markAsPublished(event.getId(), Instant.now());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void handlePublishFailure(OutboxEvent event, String errorMessage) {
        outboxEventRepository.recordPublishFailure(event.getId(), errorMessage, maxRetry);
    }
}
