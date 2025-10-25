package org.apache.ignite.internal.replicator.message;

import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

@Transferable(ReplicaMessageGroup.GET_ESTIMATED_SIZE_WITH_MODIFIED_TS_MESSAGE)
public interface PartitionModificationInfoMessage extends NetworkMessage {
    /** Table id. */
    int tableId();

    /** Partition id. */
    int partId();

    /** Estimated size. */
    long estimatedSize();

    /** Modification counter. */
    long lastModificationCounter();
}
