package org.apache.ignite.internal.replicator.message;

import org.apache.ignite.network.annotations.Transferable;

@Transferable(ReplicaMessageGroup.AWAIT_REPLICA_REQUEST)
public interface AwaitReplicaRequest extends ReplicaRequest {
}
