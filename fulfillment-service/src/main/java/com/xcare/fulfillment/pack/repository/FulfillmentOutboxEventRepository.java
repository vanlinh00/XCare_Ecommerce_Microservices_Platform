package com.xcare.fulfillment.pack.repository;

import com.xcare.fulfillment.pack.domain.FulfillmentOutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface FulfillmentOutboxEventRepository extends JpaRepository<FulfillmentOutboxEvent, UUID> {

    /**
     * Polling Outbox Events sử dụng FOR UPDATE SKIP LOCKED của PostgreSQL 16.
     * Ngăn chặn nhiều worker/pod tranh chấp cùng 1 bản ghi outbox event.
     */
    @Query(value = """
            SELECT * FROM fulfillment_outbox
            WHERE status = 'PENDING' AND retry_count < :maxRetry
            ORDER BY created_at ASC
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<FulfillmentOutboxEvent> findPendingEventsWithSkipLocked(
            @Param("batchSize") int batchSize,
            @Param("maxRetry") int maxRetry
    );
}
