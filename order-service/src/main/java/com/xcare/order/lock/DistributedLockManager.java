package com.xcare.order.lock;

import com.xcare.order.exception.DistributedLockException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Distributed Lock Manager utilizing Redisson 3.42.0 and Redis 7.2.
 *
 * Implements deterministic lock key ordering to prevent distributed deadlocks
 * during concurrent multi-item checkout operations.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DistributedLockManager {

    private final RedissonClient redissonClient;

    @Value("${xcare.lock.wait-time-seconds:5}")
    private long defaultWaitTime;

    @Value("${xcare.lock.lease-time-seconds:10}")
    private long defaultLeaseTime;

    private static final String INVENTORY_LOCK_PREFIX = "lock:xcare:inventory:hub:";

    /**
     * Executes a supplier callback under a Redisson multi-lock on all SKUs for a specific pharmacy hub.
     * Lock keys are sorted lexicographically to eliminate distributed deadlock risk.
     *
     * @param pharmacyHubId Pharmacy Store / Warehouse ID
     * @param skus           List of product SKUs in the order
     * @param action         The business transaction supplier to execute safely
     * @param <T>            Return type
     * @return Execution result
     */
    public <T> T executeWithInventoryLock(String pharmacyHubId, List<String> skus, Supplier<T> action) {
        // 1. Sort SKUs lexicographically to enforce deterministic locking order and prevent circular deadlocks
        List<String> sortedSkus = new ArrayList<>(skus);
        Collections.sort(sortedSkus);

        // 2. Build Redisson locks for each SKU within the pharmacy hub scope
        List<RLock> locks = sortedSkus.stream()
                .distinct()
                .map(sku -> redissonClient.getLock(INVENTORY_LOCK_PREFIX + pharmacyHubId + ":sku:" + sku))
                .toList();

        RLock multiLock = redissonClient.getMultiLock(locks.toArray(new RLock[0]));

        boolean isLocked = false;
        try {
            log.debug("Attempting to acquire Redisson multi-lock for hub [{}] and skus {}", pharmacyHubId, sortedSkus);
            isLocked = multiLock.tryLock(defaultWaitTime, defaultLeaseTime, TimeUnit.SECONDS);

            if (!isLocked) {
                log.warn("Failed to acquire Redisson multi-lock within {}s for hub [{}] and skus {}",
                        defaultWaitTime, pharmacyHubId, sortedSkus);
                throw new DistributedLockException(
                        "Unable to secure inventory reservation lock for requested pharmacy products. Please retry."
                );
            }

            log.debug("Successfully acquired distributed lock for hub [{}] and skus {}", pharmacyHubId, sortedSkus);
            return action.get();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DistributedLockException("Lock acquisition interrupted: " + e.getMessage());
        } finally {
            if (isLocked) {
                try {
                    multiLock.unlock();
                    log.debug("Released distributed multi-lock for hub [{}] and skus {}", pharmacyHubId, sortedSkus);
                } catch (Exception ex) {
                    log.error("Error releasing Redisson multi-lock for hub [{}]", pharmacyHubId, ex);
                }
            }
        }
    }
}
