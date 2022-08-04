package org.apache.ignite.internal.table.distributed.replication.request;

import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Read write single row replica request.
 */
@Transferable(TableMessageGroup.RW_SINGLE_ROW_REPLICA_REQUEST)
public interface ReadWriteSingleRowReplicaRequest extends SingleRowReplicaRequest, ReadWriteReplicaRequest {
}
