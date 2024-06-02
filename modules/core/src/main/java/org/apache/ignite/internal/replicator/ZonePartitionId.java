package org.apache.ignite.internal.replicator;

import java.util.Objects;

// TODO KKK replace by the feature-branch compatible version
public class ZonePartitionId implements ReplicationGroupId {

    private final int zoneId;

    private final int partitionId;

    public ZonePartitionId(int zoneId, int partitionId) {
        this.zoneId = zoneId;
        this.partitionId = partitionId;
    }

    public int zoneId() {
        return zoneId;
    }

    public int partitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ZonePartitionId that = (ZonePartitionId) o;
        return zoneId == that.zoneId && partitionId == that.partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(zoneId, partitionId);
    }

    @Override
    public String toString() {
        return zoneId + "_part_" + partitionId;
    }
}
