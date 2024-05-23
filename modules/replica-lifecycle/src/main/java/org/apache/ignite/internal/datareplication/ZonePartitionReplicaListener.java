package org.apache.ignite.internal.datareplication;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRowImpl;

public class ZonePartitionReplicaListener implements ReplicaListener {

    @Override
    public CompletableFuture<ReplicaResult> invoke(ReplicaRequest request, String senderId) {
        var res =
                new BinaryRowImpl(1,
                        new BinaryTupleBuilder(2).appendLong(1).appendInt(-1).build());
        return CompletableFuture.completedFuture(new ReplicaResult(
                res,
                CompletableFuture.completedFuture(res)));
    }
}
