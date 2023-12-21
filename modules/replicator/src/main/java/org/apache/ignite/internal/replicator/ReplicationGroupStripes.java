package org.apache.ignite.internal.replicator;

import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;

/**
 * Logic used to calculate a stripe (or its index) to use to handle a replication request by its replication group ID.
 */
public class ReplicationGroupStripes {
    /**
     * Returns an executor that will execute requests belonging to the given replication group ID.
     *
     * @param groupId ID of the group.
     * @param stripedExecutor Striped executor from which to take a stripe.
     */
    public static ExecutorService stripeFor(ReplicationGroupId groupId, StripedThreadPoolExecutor stripedExecutor) {
        return stripedExecutor.commandExecutor(groupId.hashCode());
    }
}
