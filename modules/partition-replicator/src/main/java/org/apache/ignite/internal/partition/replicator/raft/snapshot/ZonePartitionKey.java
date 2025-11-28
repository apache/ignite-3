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

package org.apache.ignite.internal.partition.replicator.raft.snapshot;

import java.util.Objects;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tostring.S;

/**
 * Uniquely identifies a partition. This is a pair of zone ID and partition number (aka partition ID).
 */
public class ZonePartitionKey implements PartitionKey {
    private final int zoneId;

    private final int partitionId;

    /**
     * Returns ID of the zone.
     */
    public int zoneId() {
        return zoneId;
    }

    /**
     * Returns partition ID.
     */
    @Override
    public int partitionId() {
        return partitionId;
    }

    /**
     * Constructs a new partition key.
     */
    public ZonePartitionKey(int zoneId, int partitionId) {
        this.zoneId = zoneId;
        this.partitionId = partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ZonePartitionKey that = (ZonePartitionKey) o;
        return partitionId == that.partitionId && zoneId == that.zoneId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(zoneId, partitionId);
    }

    @Override
    public String toString() {
        return S.toString(ZonePartitionKey.class, this);
    }

    @Override
    public ReplicationGroupId toReplicationGroupId() {
        return new ZonePartitionId(zoneId, partitionId);
    }
}
