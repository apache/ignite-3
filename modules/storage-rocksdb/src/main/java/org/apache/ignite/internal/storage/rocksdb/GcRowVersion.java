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

package org.apache.ignite.internal.storage.rocksdb;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Row versions for garbage collection.
 */
final class GcRowVersion implements GcEntry {
    @IgniteToStringInclude
    private final RowId rowId;

    @IgniteToStringInclude
    private final HybridTimestamp rowTimestamp;

    GcRowVersion(RowId rowId, HybridTimestamp rowTimestamp) {
        this.rowId = rowId;
        this.rowTimestamp = rowTimestamp;
    }

    @Override
    public RowId getRowId() {
        return rowId;
    }

    @Override
    public HybridTimestamp getTimestamp() {
        return rowTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GcRowVersion that = (GcRowVersion) o;

        return rowId.equals(that.rowId) && rowTimestamp.equals(that.rowTimestamp);
    }

    @Override
    public int hashCode() {
        int result = rowId.hashCode();

        result = 31 * result + rowTimestamp.hashCode();

        return result;
    }

    @Override
    public String toString() {
        return S.toString(GcRowVersion.class, this);
    }
}
