package org.apache.ignite.internal.table.distributed.replicator;

import org.apache.ignite.internal.replicator.ReplicationGroupId;

public class ZoneReplicaGroupId implements ReplicationGroupId {

    public final int zoneId;

    public final int partId;

    public ZoneReplicaGroupId(int zoneId, int partId) {
        this.zoneId = zoneId;
        this.partId = partId;
    }

    @Override
    public String toString() {
        return zoneId + "_part_" + partId;
    }
}
