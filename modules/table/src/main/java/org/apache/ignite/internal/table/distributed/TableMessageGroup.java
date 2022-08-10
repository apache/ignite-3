package org.apache.ignite.internal.table.distributed;

import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSwapRowReplicaRequest;
import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Message group for the table module.
 */
@MessageGroup(groupType = 9, groupName = "TableMessages")
public class TableMessageGroup {
    /**
     * Message type for {@link org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest}.
     */
    public static final short RW_SINGLE_ROW_REPLICA_REQUEST = 0;

    /**
     * Message type for {@link org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowReplicaRequest}.
     */
    public static final short RW_MULTI_ROW_REPLICA_REQUEST = 1;

    /**
     * Message type for {@link ReadWriteSwapRowReplicaRequest}.
     */
    public static final short RW_DUAL_ROW_REPLICA_REQUEST = 2;
}
