package org.apache.ignite.internal.table.distributed.replication.request;

import java.util.Collection;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.network.ClusterNode;

public class ReplicaRequestParameters {
    private InternalTransaction tx;

    private Collection<BinaryRow> binaryRows;

    private String groupId;

    private ClusterNode primaryReplica;

    private Long term;

    public ReplicaRequestParameters(InternalTransaction tx, Collection<BinaryRow> binaryRows, String groupId, ClusterNode primaryReplica,
            Long term) {
        this.tx = tx;
        this.binaryRows = binaryRows;
        this.groupId = groupId;
        this.primaryReplica = primaryReplica;
        this.term = term;
    }

    public ReplicaRequestParameters(InternalTransaction tx, String groupId, ClusterNode primaryReplica, Long term) {
        this.tx = tx;
        this.groupId = groupId;
        this.primaryReplica = primaryReplica;
        this.term = term;
    }

    public InternalTransaction tx() {
        return tx;
    }

    public Collection<BinaryRow> binaryRows() {
        return binaryRows;
    }

    public String groupId() {
        return groupId;
    }

    public ClusterNode replica() {
        return primaryReplica;
    }

    public Long term() {
        return term;
    }
}
