package com.xcare.procurement.repository;

import com.xcare.procurement.domain.entity.StockTransferSaga;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface StockTransferSagaRepository extends JpaRepository<StockTransferSaga, UUID> {
    Optional<StockTransferSaga> findByTransferId(String transferId);
}
