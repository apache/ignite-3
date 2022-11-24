package org.apache.ignite.internal.replicator.message;

import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.exception.ReplicaUnavailableException;
import org.apache.ignite.network.annotations.Transferable;

/**
 * This request is sent when some request fails in {@link ReplicaService} with an {@link ReplicaUnavailableException}
 * due to the {@link Replica} has not created in {@link ReplicaManager}.
 */
@Transferable(ReplicaMessageGroup.AWAIT_REPLICA_REQUEST)
public interface AwaitReplicaRequest extends ReplicaRequest {
}
