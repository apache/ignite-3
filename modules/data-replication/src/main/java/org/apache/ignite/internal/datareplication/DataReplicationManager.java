package org.apache.ignite.internal.datareplication;

import static org.apache.ignite.internal.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * The main responsibilities of this class:
 * - Start the appropriate replication nodes (RAFT nodes at the moment) and logic replicas on the zone creation
 * - Stop the same entities on the node removing
 * - Support the rebalance mechanism and start the new replication entities when the rebalance triggers occurred.
 */
public class DataReplicationManager implements IgniteComponent {

    /* Feature flag for zone based collocation track */
    // TODO IGNITE-22115 remove it
    public static boolean ENABLED = getBoolean("IGNITE_ZONE_BASED_REPLICATION", false);

    @Override
    public CompletableFuture<Void> startAsync() {
        if (!ENABLED) {
            return nullCompletedFuture();
        }

        // TODO: IGNITE-22231 Will be replaced by the real code.
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync() {
        if (!ENABLED) {
            return nullCompletedFuture();
        }

        // TODO: IGNITE-22231 Will be replaced by the real code.
        return nullCompletedFuture();
    }
}
