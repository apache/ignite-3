package org.apache.ignite.internal.replicator;

import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;

/**
 * Logic used to calculate a stripe (or its index) to use to handle a replication request by its replication group ID.
 */
public class ReplicationGroupStripes {
    public static int stripeIndex(ReplicationGroupId groupId) {
        return groupId.hashCode();
    }

    public static ExecutorService stripeFor(ReplicationGroupId groupId, StripedThreadPoolExecutor stripedExecutor) {
        return stripedExecutor.commandExecutor(stripeIndex(groupId));
    }
}
