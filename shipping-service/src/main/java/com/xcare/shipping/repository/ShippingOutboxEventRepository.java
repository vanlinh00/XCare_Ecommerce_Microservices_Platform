package com.xcare.shipping.repository;

import com.xcare.shipping.domain.entity.ShippingOutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ShippingOutboxEventRepository extends JpaRepository<ShippingOutboxEvent, UUID> {

    @Query(value = """
        SELECT * FROM shipping_outbox 
        WHERE status = 'PENDING' AND retry_count < 5 
        ORDER BY created_at ASC 
        LIMIT :batchSize 
        FOR UPDATE SKIP LOCKED
    """, nativeQuery = true)
    List<ShippingOutboxEvent> findPendingEventsSkipLocked(int batchSize);
}
