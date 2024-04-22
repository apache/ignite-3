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

package org.apache.ignite.internal.storage.index;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_FIRST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** For {@link StorageHashIndexDescriptor} testing. */
public class StorageIndexDescriptorTest {
    private static final String COLUMN_NAME = "INT_VAL";

    private final AtomicInteger nextId = new AtomicInteger();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIsPk(boolean pk) {
        CatalogTableDescriptor tableDescriptor = createTableDescriptor();

        int tableId = tableDescriptor.id();
        int indexId = pk ? tableDescriptor.primaryKeyIndexId() : nextId.getAndIncrement();

        CatalogHashIndexDescriptor pkHashIndexDescriptor = createHashIndexDescriptor(indexId, tableId);
        CatalogSortedIndexDescriptor pkSortedIndexDescriptor = createSortedIndexDescriptor(indexId, tableId);

        assertEquals(pk, StorageIndexDescriptor.create(tableDescriptor, pkHashIndexDescriptor).isPk());
        assertEquals(pk, StorageIndexDescriptor.create(tableDescriptor, pkSortedIndexDescriptor).isPk());
    }

    private CatalogTableDescriptor createTableDescriptor() {
        return new CatalogTableDescriptor(
                nextId.getAndIncrement(),
                nextId.getAndIncrement(),
                nextId.getAndIncrement(),
                "TABLE_NAME",
                nextId.getAndIncrement(),
                List.of(new CatalogTableColumnDescriptor(COLUMN_NAME, INT32, false, 0, 0, 0, null)),
                List.of(COLUMN_NAME),
                List.of(COLUMN_NAME),
                DEFAULT_STORAGE_PROFILE
        );
    }

    private static CatalogHashIndexDescriptor createHashIndexDescriptor(int indexId, int tableId) {
        return new CatalogHashIndexDescriptor(
                indexId,
                "INDEX_NAME_" + indexId,
                tableId,
                false,
                AVAILABLE,
                1,
                List.of(COLUMN_NAME)
        );
    }

    private static CatalogSortedIndexDescriptor createSortedIndexDescriptor(int indexId, int tableId) {
        return new CatalogSortedIndexDescriptor(
                indexId,
                "INDEX_NAME_" + indexId,
                tableId,
                false,
                AVAILABLE,
                1,
                List.of(new CatalogIndexColumnDescriptor(COLUMN_NAME, ASC_NULLS_FIRST))
        );
    }
}
