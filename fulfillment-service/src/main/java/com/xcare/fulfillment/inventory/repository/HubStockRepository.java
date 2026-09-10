package com.xcare.fulfillment.inventory.repository;

import com.xcare.fulfillment.inventory.domain.HubStock;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface HubStockRepository extends JpaRepository<HubStock, UUID> {

    Optional<HubStock> findByHubIdAndSku(String hubId, String sku);
}
