package com.xcare.fulfillment.inventory.repository;

import com.xcare.fulfillment.inventory.domain.FailedInventoryCommand;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface FailedInventoryCommandRepository extends JpaRepository<FailedInventoryCommand, Long> {

    List<FailedInventoryCommand> findByTransferId(String transferId);

    List<FailedInventoryCommand> findByOrderNumber(String orderNumber);

    List<FailedInventoryCommand> findByStatus(String status);

    Optional<FailedInventoryCommand> findByTopicAndKafkaPartitionAndKafkaOffset(String topic, Integer kafkaPartition, Long kafkaOffset);
}
