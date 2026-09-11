package com.xcare.procurement.repository;

import com.xcare.procurement.domain.entity.ProcurementOutboxEvent;
import com.xcare.procurement.domain.enums.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ProcurementOutboxEventRepository extends JpaRepository<ProcurementOutboxEvent, UUID> {

    @Query(value = "SELECT * FROM procurement_outbox_events WHERE status = :status ORDER BY created_at ASC LIMIT :limit FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<ProcurementOutboxEvent> findPendingEventsWithSkipLocked(@Param("status") String status, @Param("limit") int limit);

    List<ProcurementOutboxEvent> findByStatus(OutboxStatus status);
}
