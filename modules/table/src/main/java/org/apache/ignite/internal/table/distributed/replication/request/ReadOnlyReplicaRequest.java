package org.apache.ignite.internal.table.distributed.replication.request;

import java.util.UUID;
import org.apache.ignite.network.NetworkMessage;

/**
 * RW replica request.
 */
// TODO: extends ReplicaRequest issue.
public interface ReadOnlyReplicaRequest extends NetworkMessage { //extends ReplicaRequest {

    UUID transactionId();
}
