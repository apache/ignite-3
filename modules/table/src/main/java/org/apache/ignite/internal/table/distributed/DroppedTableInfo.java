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

package org.apache.ignite.internal.table.distributed;

import org.apache.ignite.internal.tostring.S;

/** Information about the dropped table. */
final class DroppedTableInfo {
    private final int tableId;

    /** Catalog version in which the table was removed from the catalog. */
    private final int tableRemovalCatalogVersion;

    DroppedTableInfo(int tableId, int tableRemovalCatalogVersion) {
        this.tableId = tableId;
        this.tableRemovalCatalogVersion = tableRemovalCatalogVersion;
    }

    public int tableId() {
        return tableId;
    }

    public int tableRemovalCatalogVersion() {
        return tableRemovalCatalogVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DroppedTableInfo other = (DroppedTableInfo) o;

        return tableId == other.tableId && tableRemovalCatalogVersion == other.tableRemovalCatalogVersion;
    }

    @Override
    public int hashCode() {
        int result = tableId;
        result = 31 * result + tableRemovalCatalogVersion;
        return result;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
