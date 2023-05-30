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

import java.util.UUID;
import org.apache.ignite.internal.tostring.S;

/**
 * ID of the index build task.
 */
class BuildIndexTaskId {
    private final int tableId;

    private final UUID indexId;

    private final int partitionId;

    BuildIndexTaskId(int tableId, UUID indexId, int partitionId) {
        this.tableId = tableId;
        this.indexId = indexId;
        this.partitionId = partitionId;
    }

    int getTableId() {
        return tableId;
    }

    UUID getIndexId() {
        return indexId;
    }

    int getPartitionId() {
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

        BuildIndexTaskId that = (BuildIndexTaskId) o;

        if (partitionId != that.partitionId) {
            return false;
        }
        if (tableId != that.tableId) {
            return false;
        }
        return indexId.equals(that.indexId);
    }

    @Override
    public int hashCode() {
        int result = tableId;
        result = 31 * result + indexId.hashCode();
        result = 31 * result + partitionId;
        return result;
    }

    @Override
    public String toString() {
        return S.toString(BuildIndexTaskId.class, this);
    }
}
