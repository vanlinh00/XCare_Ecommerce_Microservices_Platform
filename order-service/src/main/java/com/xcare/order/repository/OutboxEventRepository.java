package com.xcare.order.repository;

import com.xcare.order.domain.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Fetch pending outbox records using PostgreSQL 'FOR UPDATE SKIP LOCKED'.
     * This guarantees safe concurrent execution across multiple microservice replicas
     * without deadlocks or duplicate processing.
     */
    @Query(value = """
            SELECT * FROM outbox_events
            WHERE status = 'PENDING' AND retry_count < :maxRetry
            ORDER BY created_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> findPendingEventsWithSkipLocked(
            @Param("limit") int limit,
            @Param("maxRetry") int maxRetry
    );

    @Modifying
    @Query("""
            UPDATE OutboxEvent o
            SET o.status = 'PUBLISHED', o.publishedAt = :publishedAt
            WHERE o.id = :id
            """)
    void markAsPublished(@Param("id") UUID id, @Param("publishedAt") Instant publishedAt);

    @Modifying
    @Query("""
            UPDATE OutboxEvent o
            SET o.status = CASE WHEN o.retryCount + 1 >= :maxRetry THEN 'FAILED' ELSE 'PENDING' END,
                o.retryCount = o.retryCount + 1,
                o.errorMessage = :errorMessage
            WHERE o.id = :id
            """)
    void recordPublishFailure(
            @Param("id") UUID id,
            @Param("errorMessage") String errorMessage,
            @Param("maxRetry") int maxRetry
    );
}
