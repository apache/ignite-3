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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.tostring.S;

/** Internal class for use in {@link FullStateTransferIndexChooser} for read-only indexes. */
final class ReadOnlyIndexInfo {
    private final int tableId;

    /**
     * Timestamp of activation of the catalog version in which the index got the status {@link CatalogIndexStatus#STOPPING} or the table was
     * dropped and the index had the status {@link CatalogIndexStatus#AVAILABLE}.
     */
    private final long activationTs;

    private final int indexId;

    /** Catalog version in which the index was removed from the catalog. */
    private final int indexRemovalCatalogVersion;

    ReadOnlyIndexInfo(CatalogIndexDescriptor index, long activationTs, int indexRemovalCatalogVersion) {
        this(index.tableId(), activationTs, index.id(), indexRemovalCatalogVersion);
    }

    ReadOnlyIndexInfo(int tableId, long activationTs, int indexId, int indexRemovalCatalogVersion) {
        this.tableId = tableId;
        this.activationTs = activationTs;
        this.indexId = indexId;
        this.indexRemovalCatalogVersion = indexRemovalCatalogVersion;
    }

    int tableId() {
        return tableId;
    }

    long activationTs() {
        return activationTs;
    }

    int indexId() {
        return indexId;
    }

    int indexRemovalCatalogVersion() {
        return indexRemovalCatalogVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReadOnlyIndexInfo other = (ReadOnlyIndexInfo) o;

        return tableId == other.tableId
                && activationTs == other.activationTs
                && indexId == other.indexId;
    }

    @Override
    public int hashCode() {
        int result = tableId;
        result = 31 * result + (int) (activationTs ^ (activationTs >>> 32));
        result = 31 * result + indexId;
        return result;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
