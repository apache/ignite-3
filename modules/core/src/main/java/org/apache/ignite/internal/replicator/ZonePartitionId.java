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

import java.util.regex.Pattern;

/**
 * The class is used to identify a zone replication group id for a given partition.
 */
public class ZonePartitionId implements PartitionGroupId {
    private static final Pattern DELIMITER_PATTERN = Pattern.compile("_part_");

    private final int zoneId;

    private final int partId;

    /**
     * The constructor.
     *
     * @param zoneId Zone id.
     * @param partId Partition id.
     */
    public ZonePartitionId(int zoneId, int partId) {
        this.zoneId = zoneId;
        this.partId = partId;
    }

    /**
     * Get the zone id.
     *
     * @return Zone id.
     */
    public int zoneId() {
        return zoneId;
    }

    @Override
    public int objectId() {
        return zoneId;
    }

    /**
     * Get the partition id.
     *
     * @return Partition id.
     */
    @Override
    public int partitionId() {
        return partId;
    }

    /**
     * Converts a string representation of zone partition id to the object.
     *
     * @param str String representation.
     * @return An zone partition id.
     */
    public static ZonePartitionId fromString(String str) {
        String[] parts = DELIMITER_PATTERN.split(str);

        return new ZonePartitionId(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
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

        ZonePartitionId that = (ZonePartitionId) o;

        return zoneId == that.zoneId && partId == that.partId;
    }

    @Override
    public int hashCode() {
        int result = 1;

        result = 31 * result + zoneId;
        result = 31 * result + partId;

        return result;
    }
}
