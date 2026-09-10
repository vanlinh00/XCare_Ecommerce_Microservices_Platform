package com.xcare.fulfillment.inventory.repository;

import com.xcare.fulfillment.inventory.domain.HubStock;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface HubStockRepository extends JpaRepository<HubStock, UUID> {

    Optional<HubStock> findByHubIdAndSku(String hubId, String sku);

    /**
     * Senior Engineering Pattern: Atomic SQL Query trực tiếp trên PostgreSQL.
     * KHÔNG DÙNG REDIS: Đảm bảo ACID transaction và chống Race Condition tuyệt đối ở tầng CSDL.
     * Hoàn trả số lượng: available_quantity = available_quantity + qty, reserved_quantity = GREATEST(0, reserved_quantity - qty).
     */
    @Modifying
    @Query(value = """
        UPDATE hub_stocks 
        SET available_quantity = available_quantity + :qty,
            reserved_quantity = GREATEST(0, reserved_quantity - :qty),
            updated_at = NOW()
        WHERE hub_id = :hubId AND sku = :sku
    """, nativeQuery = true)
    int releaseStockAtomic(@Param("hubId") String hubId, @Param("sku") String sku, @Param("qty") int qty);

    /**
     * Atomic SQL Query trừ/giữ kho cho Bước 2.
     */
    @Modifying
    @Query(value = """
        UPDATE hub_stocks 
        SET available_quantity = available_quantity - :qty,
            reserved_quantity = reserved_quantity + :qty,
            updated_at = NOW()
        WHERE hub_id = :hubId AND sku = :sku AND available_quantity >= :qty
    """, nativeQuery = true)
    int reserveStockAtomic(@Param("hubId") String hubId, @Param("sku") String sku, @Param("qty") int qty);
}
