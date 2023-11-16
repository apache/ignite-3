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

import static org.apache.ignite.internal.index.IndexManagementUtils.getNotEmptyTableIndexes;

import java.util.List;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.jetbrains.annotations.Nullable;

/** An auxiliary class for collecting indexes, for example, for partition update operations. */
// TODO: IGNITE-20863 думаю документацию надо добавить
class TableIndexes {
    private final List<CatalogIndexDescriptor>[] tableIndexesByVersion;

    private final int[] indexByVersion;

    TableIndexes(CatalogService catalogService, int catalogVersionFrom, int catalogVersionTo, int tableId) {
        assert catalogVersionFrom < catalogVersionTo : "from=" + catalogVersionFrom + ", to=" + catalogVersionTo;

        int versionCount = (catalogVersionTo - catalogVersionFrom) + 1;

        indexByVersion = new int[versionCount];

        tableIndexesByVersion = new List[versionCount];

        for (int i = 0; i < versionCount; i++) {
            tableIndexesByVersion[i] = getNotEmptyTableIndexes(catalogService, i + catalogVersionFrom, tableId);
        }
    }

    int versionCount() {
        return tableIndexesByVersion.length;
    }

    int indexCount(int versionIndex) {
        return tableIndexesByVersion[versionIndex].size();
    }

    int handledIndexCount(int versionIndex) {
        return indexByVersion[versionIndex];
    }

    CatalogIndexDescriptor advanceNextIndex(int versionIndex) {
        int i = indexByVersion[versionIndex];

        CatalogIndexDescriptor index = tableIndexesByVersion[versionIndex].get(i);

        indexByVersion[versionIndex]++;

        return index;
    }

    @Nullable CatalogIndexDescriptor findLatestVersion(CatalogIndexDescriptor index, int versionIndexFrom) {
        CatalogIndexDescriptor latest = index;

        for (int versionIndex = versionIndexFrom; versionIndex < versionCount(); versionIndex++) {
            CatalogIndexDescriptor peeked = peekNextIndex(versionIndex);

            if (peeked != null && peeked.id() == latest.id()) {
                indexByVersion[versionIndex]++;

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

    private @Nullable CatalogIndexDescriptor peekNextIndex(int versionIndex) {
        if (handledIndexCount(versionIndex) == indexCount(versionIndex)) {
            return null;
        }

        int i = indexByVersion[versionIndex];

        return tableIndexesByVersion[versionIndex].get(i);
    }
}
