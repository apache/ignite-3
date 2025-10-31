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

package org.apache.ignite.internal.storage;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;

/**
 * Base test for MV table storages.
 */
@SuppressWarnings("JUnitTestMethodInProductSource")
public abstract class BaseMvTableStorageTest extends BaseMvStoragesTest {
    protected static final String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;

    protected static final String TABLE_NAME = "FOO";

    protected static final int TABLE_ID = 1;

    protected static final String PK_INDEX_NAME = pkIndexName(TABLE_NAME);

    protected static final String SORTED_INDEX_NAME = "SORTED_IDX";

    protected static final String HASH_INDEX_NAME = "HASH_IDX";

    protected static final int PARTITION_ID = 0;

    /** Partition id for 0 storage. */
    protected static final int PARTITION_ID_0 = 10;

    /** Partition id for 1 storage. */
    protected static final int PARTITION_ID_1 = 9;

    protected static final int COMMIT_TABLE_ID = 999;

    protected MvTableStorage tableStorage;

    protected StorageSortedIndexDescriptor sortedIdx;

    protected StorageHashIndexDescriptor hashIdx;

    protected StorageHashIndexDescriptor pkIdx;

    protected final Catalog catalog = mock(Catalog.class);

    protected final StorageIndexDescriptorSupplier indexDescriptorSupplier = new StorageIndexDescriptorSupplier() {
        @Override
        public @Nullable StorageIndexDescriptor get(int indexId) {
            CatalogIndexDescriptor indexDescriptor = catalog.index(indexId);

            if (indexDescriptor == null) {
                return null;
            }

            CatalogTableDescriptor tableDescriptor = catalog.table(indexDescriptor.tableId());

            assertThat(tableDescriptor, is(notNullValue()));

            return StorageIndexDescriptor.create(tableDescriptor, indexDescriptor);
        }
    };

    /** Used to represent rows in tests. */
    protected class TestRow {
        final RowId rowId;

        final BinaryRow row;

        final HybridTimestamp timestamp;

        TestRow(RowId rowId, BinaryRow row) {
            this.rowId = rowId;
            this.row = row;
            this.timestamp = clock.now();
        }
    }

    @AfterEach
    protected void tearDown() throws Exception {
        if (tableStorage != null) {
            tableStorage.close();
        }
    }

    /** Creates a table storage instance for testing. */
    protected abstract MvTableStorage createMvTableStorage();

    private static void createTestTableAndIndexes(Catalog catalog) {
        int id = 0;

        int schemaId = id++;
        int tableId = id++;
        int zoneId = id++;
        int sortedIndexId = id++;
        int hashIndexId = id++;
        int pkIndexId = id++;

        List<CatalogTableColumnDescriptor> columns = List.of(
                CatalogUtils.fromParams(ColumnParams.builder().name("INTKEY").type(INT32).build()),
                CatalogUtils.fromParams(ColumnParams.builder().name("STRKEY").length(100).type(STRING).build()),
                CatalogUtils.fromParams(ColumnParams.builder().name("INTVAL").type(INT32).build()),
                CatalogUtils.fromParams(ColumnParams.builder().name("STRVAL").length(100).type(STRING).build())
        );
        CatalogTableDescriptor tableDescriptor = CatalogTableDescriptor.builder()
                .id(tableId)
                .schemaId(schemaId)
                .primaryKeyIndexId(pkIndexId)
                .name(TABLE_NAME)
                .zoneId(zoneId)
                .newColumns(columns)
                .primaryKeyColumns(IntList.of(0))
                .storageProfile(CatalogService.DEFAULT_STORAGE_PROFILE)
                .build();

        CatalogSortedIndexDescriptor sortedIndex = new CatalogSortedIndexDescriptor(
                sortedIndexId,
                SORTED_INDEX_NAME,
                tableId,
                false,
                AVAILABLE,
                List.of(new CatalogIndexColumnDescriptor(1, ASC_NULLS_LAST)),
                false
        );

        CatalogHashIndexDescriptor hashIndex = new CatalogHashIndexDescriptor(
                hashIndexId,
                HASH_INDEX_NAME,
                tableId,
                true,
                AVAILABLE,
                IntList.of(1),
                false
        );

        CatalogIndexDescriptor pkIndex = new CatalogHashIndexDescriptor(
                pkIndexId,
                PK_INDEX_NAME,
                tableId,
                true,
                AVAILABLE,
                IntList.of(0),
                true
        );

        when(catalog.table(eq(SCHEMA_NAME), eq(TABLE_NAME))).thenReturn(tableDescriptor);
        when(catalog.aliveIndex(eq(SCHEMA_NAME), eq(SORTED_INDEX_NAME))).thenReturn(sortedIndex);
        when(catalog.aliveIndex(eq(SCHEMA_NAME), eq(HASH_INDEX_NAME))).thenReturn(hashIndex);
        when(catalog.aliveIndex(eq(SCHEMA_NAME), eq(PK_INDEX_NAME))).thenReturn(pkIndex);

        when(catalog.table(eq(tableId))).thenReturn(tableDescriptor);
        when(catalog.index(eq(sortedIndexId))).thenReturn(sortedIndex);
        when(catalog.index(eq(hashIndexId))).thenReturn(hashIndex);
        when(catalog.index(eq(pkIndexId))).thenReturn(pkIndex);
    }

    protected static <T> List<T> getAll(Cursor<T> cursor) {
        try (cursor) {
            return cursor.stream().collect(toList());
        }
    }

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    protected final void initialize() {
        createTestTableAndIndexes(catalog);

        this.tableStorage = createMvTableStorage();

        CatalogTableDescriptor catalogTableDescriptor = catalog.table(SCHEMA_NAME, TABLE_NAME);
        assertNotNull(catalogTableDescriptor);

        CatalogIndexDescriptor catalogSortedIndexDescriptor = catalog.aliveIndex(SCHEMA_NAME, SORTED_INDEX_NAME);
        CatalogIndexDescriptor catalogHashIndexDescriptor = catalog.aliveIndex(SCHEMA_NAME, HASH_INDEX_NAME);
        CatalogIndexDescriptor catalogPkIndexDescriptor = catalog.aliveIndex(SCHEMA_NAME, PK_INDEX_NAME);

        assertNotNull(catalogSortedIndexDescriptor);
        assertNotNull(catalogHashIndexDescriptor);
        assertNotNull(catalogPkIndexDescriptor);

        sortedIdx = new StorageSortedIndexDescriptor(catalogTableDescriptor, (CatalogSortedIndexDescriptor) catalogSortedIndexDescriptor);
        hashIdx = new StorageHashIndexDescriptor(catalogTableDescriptor, (CatalogHashIndexDescriptor) catalogHashIndexDescriptor);
        pkIdx = (StorageHashIndexDescriptor) StorageIndexDescriptor.create(catalogTableDescriptor, catalogPkIndexDescriptor);
    }

    /**
     * Retrieves or creates a multi-versioned partition storage.
     */
    protected MvPartitionStorage getOrCreateMvPartition(int partitionId) {
        return getOrCreateMvPartition(tableStorage, partitionId);
    }

    /**
     * Returns an already created index or creates a new one.
     *
     * @param partitionId Partition ID.
     * @param indexDescriptor Storage index descriptor.
     * @param built {@code True} if index building needs to be completed.
     * @see #completeBuiltIndexes(int, IndexStorage...)
     */
    protected <T extends IndexStorage> T getOrCreateIndex(
            int partitionId,
            StorageIndexDescriptor indexDescriptor,
            boolean built
    ) {
        if (indexDescriptor instanceof StorageHashIndexDescriptor) {
            tableStorage.createHashIndex(partitionId, (StorageHashIndexDescriptor) indexDescriptor);
        } else {
            tableStorage.createSortedIndex(partitionId, (StorageSortedIndexDescriptor) indexDescriptor);
        }

        IndexStorage indexStorage = tableStorage.getIndex(partitionId, indexDescriptor.id());

        assertNotNull(indexStorage, "index=" + indexDescriptor);

        if (indexStorage.getNextRowIdToBuild() != null && built) {
            completeBuiltIndexes(partitionId, indexStorage);
        }

        return (T) indexStorage;
    }

    /**
     * Returns an already created index or creates a new one with the completion of building.
     *
     * @param partitionId Partition ID.
     * @param indexDescriptor Storage index descriptor.
     * @see #completeBuiltIndexes(int, IndexStorage...)
     */
    protected <T extends IndexStorage> T getOrCreateIndex(
            int partitionId,
            StorageIndexDescriptor indexDescriptor
    ) {
        return getOrCreateIndex(partitionId, indexDescriptor, true);
    }

    /** Completes the building of indexes. */
    protected void completeBuiltIndexes(int partitionId, IndexStorage... indexStorages) {
        MvPartitionStorage partitionStorage = getOrCreateMvPartition(partitionId);

        assertNotNull(partitionStorage, "partitionId=" + partitionId);

        TestStorageUtils.completeBuiltIndexes(partitionStorage, indexStorages);
    }

    protected void recreateTableStorage() throws Exception {
        tableStorage.close();

        tableStorage = createMvTableStorage();
    }
}
