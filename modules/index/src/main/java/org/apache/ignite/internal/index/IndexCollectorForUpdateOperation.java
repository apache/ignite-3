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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Collects table indexes from a catalog between two versions of the catalog to perform a partition update operation on transaction.
 *
 * <p>An approximate algorithm for collecting indexes:</p>
 * <ul>
 *     <li>1. We go through all the requested versions of the catalog in ascending order by version.</li>
 *     <li>2. We go through all the table indexes of the catalog version in ascending order by index ID.</li>
 *     <li>2.1 If the index is available, then we find its latest version in the remaining catalog versions and add it to the result. At the
 *     same time, we will skip this index in the remaining versions of the catalogs. If the index was deleted in the remaining versions of
 *     the catalog, we must still add it so as not to violate the consistency of the transaction.</li>
 *     <li>2.2 If the index is registered, then we find its latest version in the remaining catalog versions and add it to the result. At
 *     the same time, we will skip this index in the remaining versions of the catalogs. If the index was deleted in the remaining versions
 *     of the catalog, then we don't add it to the result.</li>
 *     <li>Switch the next version of the catalog, if it is missing, then we return the result, otherwise we go to step 2.</li>
 * </ul>
 */
public class IndexCollectorForUpdateOperation {
    private final CatalogService catalogService;

    /** Constructor. */
    public IndexCollectorForUpdateOperation(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    /**
     * Collects table indexes.
     *
     * @param tableId Table ID for which indexes will be collected
     * @param catalogVersionFrom Catalog version from which index collection begins (including).
     * @param catalogVersionTo Catalog version on which the index collection ends (including).
     */
    public List<CatalogIndexDescriptor> collect(int tableId, int catalogVersionFrom, int catalogVersionTo) {
        assert catalogVersionFrom <= catalogVersionTo : "from=" + catalogVersionFrom + ", to=" + catalogVersionTo;

        if (catalogVersionFrom == catalogVersionTo) {
            return tableIndexes(catalogVersionFrom, tableId);
        }

        int versionCount = versionCount(catalogVersionFrom, catalogVersionTo);

        int[] indexes = new int[versionCount];
        List<CatalogIndexDescriptor>[] tableIndexesByVersion = tableIndexes(catalogVersionFrom, catalogVersionTo, tableId);

        List<CatalogIndexDescriptor> res = new ArrayList<>();

        for (int catalogVersionIndex = 0; catalogVersionIndex < versionCount; catalogVersionIndex++) {
            List<CatalogIndexDescriptor> tableIndexes = tableIndexesByVersion[catalogVersionIndex];

            for (int i = indexes[catalogVersionIndex]; i < tableIndexes.size(); i++) {
                CatalogIndexDescriptor index = tableIndexes.get(i);

                CatalogIndexDescriptor latestVersion = findLatest(index, catalogVersionIndex, tableIndexesByVersion, indexes);

                if (latestVersion != null) {
                    res.add(latestVersion);
                }
            }
        }

        return res;
    }

    private List<CatalogIndexDescriptor> tableIndexes(int catalogVersion, int tableId) {
        List<CatalogIndexDescriptor> indexes = catalogService.indexes(catalogVersion, tableId);

        assert !indexes.isEmpty() : "catalogVersion=" + catalogVersion + ", tableId=" + tableId;

        return indexes;
    }

    private List<CatalogIndexDescriptor>[] tableIndexes(int catalogVersionFrom, int catalogVersionTo, int tableId) {
        int catalogVersionCount = versionCount(catalogVersionFrom, catalogVersionTo);

        List<CatalogIndexDescriptor>[] tableIndexesByVersion = new List[catalogVersionCount];

        for (int i = 0; i < catalogVersionCount; i++) {
            tableIndexesByVersion[i] = tableIndexes(catalogVersionFrom + i, tableId);
        }

        return tableIndexesByVersion;
    }

    private static @Nullable CatalogIndexDescriptor findLatest(
            CatalogIndexDescriptor index,
            int catalogVersionIndexOfIndex,
            List<CatalogIndexDescriptor>[] tableIndexesByVersion,
            int[] indexes
    ) {
        CatalogIndexDescriptor latest = index;

        for (int catalogVersionIndex = catalogVersionIndexOfIndex + 1; catalogVersionIndex < indexes.length; catalogVersionIndex++) {
            List<CatalogIndexDescriptor> tableIndexes = tableIndexesByVersion[catalogVersionIndex];

            CatalogIndexDescriptor peeked = peek(tableIndexes, indexes[catalogVersionIndex]);

            if (peeked != null && peeked.id() == latest.id()) {
                indexes[catalogVersionIndex]++;

                latest = peeked;
            } else {
                if (!latest.available()) {
                    latest = null;
                }

                break;
            }
        }

        return latest;
    }

    private static int versionCount(int catalogVersionFrom, int catalogVersionTo) {
        return catalogVersionTo - catalogVersionFrom + 1;
    }

    private static @Nullable CatalogIndexDescriptor peek(List<CatalogIndexDescriptor> tableIndexes, int indexIdx) {
        return indexIdx >= tableIndexes.size() ? null : tableIndexes.get(indexIdx);
    }
}
