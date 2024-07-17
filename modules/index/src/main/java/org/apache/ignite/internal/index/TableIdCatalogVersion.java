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

/** Helper class for storing a pair of table ID and catalog version. */
class TableIdCatalogVersion implements Comparable<TableIdCatalogVersion> {
    final int tableId;

    final int catalogVersion;

    TableIdCatalogVersion(int tableId, int catalogVersion) {
        this.tableId = tableId;
        this.catalogVersion = catalogVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableIdCatalogVersion that = (TableIdCatalogVersion) o;

        return tableId == that.tableId && catalogVersion == that.catalogVersion;
    }

    @Override
    public int hashCode() {
        int result = tableId;
        result = 31 * result + catalogVersion;
        return result;
    }

    @Override
    public int compareTo(TableIdCatalogVersion o) {
        int cmp = Integer.compare(tableId, o.tableId);

        if (cmp != 0) {
            return cmp;
        }

        return Integer.compare(catalogVersion, o.catalogVersion);
    }

    @Override
    public String toString() {
        return S.toString(TableIdCatalogVersion.class, this);
    }
}
