package org.apache.ignite.internal.table.distributed.replication.request;

import java.util.Collection;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.network.annotations.Marshallable;

public interface MultipleRowReplicaRequest extends ReplicaRequest {
    @Marshallable
    Collection<BinaryRow> binaryRows();

    @Marshallable
    RequestType requestType();
}
