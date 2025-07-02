package org.apache.ignite.internal.distributionzones.rebalance;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.ComponentContext;

/**
 * Zone rebalance manager.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 After full switch to colocation we will no longer need an interface since it'll
// be exactly one implementation.
public interface DistributionZoneRebalanceEngineService {
    /**
     * Starts the rebalance engine by registering corresponding meta storage and configuration listeners.
     */
    CompletableFuture<Void> startAsync(int catalogVersion);

    /**
     * Stops the rebalance engine by unregistering meta storage watches.
     */
    void stop();
}
