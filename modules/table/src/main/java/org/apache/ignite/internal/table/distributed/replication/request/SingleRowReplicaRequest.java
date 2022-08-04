package org.apache.ignite.internal.table.distributed.replication.request;

import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.network.annotations.Marshallable;

/**
 * Single row replica request.
 */
public interface SingleRowReplicaRequest extends ReplicaRequest {
    @Marshallable
    BinaryRow binaryRow();

    @Marshallable
    RequestType requestType();
}
