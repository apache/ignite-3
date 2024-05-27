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

package org.apache.ignite.internal.index;

import org.apache.ignite.internal.tostring.S;

/**
 * {@link IndexBuildTask} ID.
 */
class IndexBuildTaskId {
    private final int zoneId;

    private final int tableId;

    private final int partitionId;

    private final int indexId;

    IndexBuildTaskId(int zoneId, int tableId, int partitionId, int indexId) {
        this.zoneId = zoneId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.indexId = indexId;
    }

    public int getZoneId() {
        return zoneId;
    }

    public int getTableId() {
        return tableId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getIndexId() {
        return indexId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexBuildTaskId that = (IndexBuildTaskId) o;

        return partitionId == that.partitionId && tableId == that.tableId && indexId == that.indexId && zoneId == that.zoneId;
    }

    @Override
    public int hashCode() {
        int result = tableId;
        result = 31 * result + partitionId;
        result = 31 * result + indexId;
        result = 31 * result + zoneId;
        return result;
    }

    @Override
    public String toString() {
        return S.toString(IndexBuildTaskId.class, this);
    }
}
