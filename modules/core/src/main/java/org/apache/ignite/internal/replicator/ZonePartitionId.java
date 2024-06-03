/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.replicator;

import java.util.Objects;

/**
 * Partition id for zone based partitions.
 */
public class ZonePartitionId implements ReplicationGroupId {

    /** Zone id. */
    private final int zoneId;

    /** Partitoin id. */
    private final int partitionId;

    /**
     * The constructor.
     *
     * @param zoneId Zone id.
     * @param partitionId Partition id.
     */
    public ZonePartitionId(int zoneId, int partitionId) {
        this.zoneId = zoneId;
        this.partitionId = partitionId;
    }

    /**
     * Get the zone id.
     *
     * @return Zone id.
     */
    public int zoneId() {
        return zoneId;
    }

    /**
     * Get the partition id.
     *
     * @return Partitiond id.
     */
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
