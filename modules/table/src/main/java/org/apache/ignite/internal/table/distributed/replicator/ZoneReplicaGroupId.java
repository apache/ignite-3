package org.apache.ignite.internal.table.distributed.replicator;

import java.util.Objects;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ZoneReplicaGroupId that = (ZoneReplicaGroupId) o;
        return zoneId == that.zoneId && partId == that.partId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(zoneId, partId);
    }
}
