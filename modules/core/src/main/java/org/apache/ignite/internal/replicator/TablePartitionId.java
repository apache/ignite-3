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
 * The class is used to identify a table replication group.
 */
public class TablePartitionId implements PartitionGroupId {
    private static final Pattern DELIMITER_PATTERN = Pattern.compile("_part_");

    /** Table id. */
    private final int tableId;

    /** Partition id. */
    private final int partId;

    /**
     * The constructor.
     *
     * @param tableId Table id.
     * @param partId Partition id.
     */
    public TablePartitionId(int tableId, int partId) {
        this.tableId = tableId;
        this.partId = partId;
    }

    /**
     * Converts a string representation of table partition id to the object.
     *
     * @param str String representation.
     * @return An table partition id.
     */
    public static TablePartitionId fromString(String str) {
        String[] parts = DELIMITER_PATTERN.split(str);

        return new TablePartitionId(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
    }

    @Override
    public int objectId() {
        return tableId;
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
     * Get the table id.
     *
     * @return Table id.
     */
    public int tableId() {
        return tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TablePartitionId that = (TablePartitionId) o;

        return partId == that.partId && tableId == that.tableId;
    }

    @Override
    public int hashCode() {
        int hash = 31 + partId;
        hash += hash * 31 + tableId;

        return hash;
    }

    @Override
    public String toString() {
        return tableId + "_part_" + partId;
    }
}
