package org.apache.ignite.internal.table.distributed;

import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Message group for the table module.
 */
@MessageGroup(groupType = 9, groupName = "TableMessages")
public class TableMessageGroup {
    /**
     * Message type for {@link RWSingleRowReplicaRequest}.
     */
    public static final short RW_SINGLE_ROW_REPLICA_REQUEST = 0;
}