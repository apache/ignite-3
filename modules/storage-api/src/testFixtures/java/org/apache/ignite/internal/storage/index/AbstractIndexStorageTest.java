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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.storage.BaseMvStoragesTest.getOrCreateMvPartition;
import static org.apache.ignite.internal.storage.util.StorageUtils.initialRowIdToBuild;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor.CatalogIndexDescriptorType;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.TestStorageUtils;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.impl.BinaryTupleRowSerializer;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Base class for index storage tests. Covers common methods, such as {@link IndexStorage#get(BinaryTuple)} or
 * {@link IndexStorage#put(IndexRow)}.
 *
 * @param <S> Type of specific index implementation.
 * @param <D> Type of index descriptor for that specific implementation.
 */
public abstract class AbstractIndexStorageTest<S extends IndexStorage, D extends StorageIndexDescriptor> extends BaseIgniteAbstractTest {
    /** Definitions of all supported column types. */
    static final List<ColumnParams> ALL_TYPES_COLUMN_PARAMS = allTypesColumnParams();

    protected static final int TEST_PARTITION = 12;

    protected static final String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;

    protected static final String TABLE_NAME = "FOO";

    private static final String PK_INDEX_NAME = pkIndexName(TABLE_NAME);

    protected static final String INDEX_NAME = "TEST_IDX";

    static List<ColumnParams> allTypesColumnParams() {
        return List.of(
                columnParamsBuilder(ColumnType.INT8).nullable(true).build(),
                columnParamsBuilder(ColumnType.INT16).nullable(true).build(),
                columnParamsBuilder(ColumnType.INT32).nullable(true).build(),
                columnParamsBuilder(ColumnType.INT64).nullable(true).build(),
                columnParamsBuilder(ColumnType.FLOAT).nullable(true).build(),
                columnParamsBuilder(ColumnType.DOUBLE).nullable(true).build(),
                columnParamsBuilder(ColumnType.UUID).nullable(true).build(),
                columnParamsBuilder(ColumnType.DATE).nullable(true).build(),
                columnParamsBuilder(ColumnType.STRING).length(100).nullable(true).build(),
                columnParamsBuilder(ColumnType.BYTE_ARRAY).length(100).nullable(true).build(),
                columnParamsBuilder(ColumnType.DECIMAL).precision(19).scale(3).nullable(true).build(),
                columnParamsBuilder(ColumnType.TIME).precision(0).nullable(true).build(),
                columnParamsBuilder(ColumnType.DATETIME).precision(6).nullable(true).build(),
                columnParamsBuilder(ColumnType.TIMESTAMP).precision(6).nullable(true).build()
        );
    }

    private final long seed = System.currentTimeMillis();

    protected final Random random = new Random(seed);

    protected MvTableStorage tableStorage;

    protected MvPartitionStorage partitionStorage;

    protected final AtomicInteger catalogId = new AtomicInteger();

    protected final Catalog catalog = mock(Catalog.class);

    protected final HybridClock clock = new HybridClockImpl();

    @BeforeEach
    void setUp() {
        log.info("Using random seed: " + seed);
    }

    /**
     * Returns a name of the column with given type.
     */
    protected static String columnName(ColumnType type) {
        return type.name();
    }

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    protected final void initialize(MvTableStorage tableStorage) {
        this.tableStorage = tableStorage;
        this.partitionStorage = getOrCreateMvPartition(tableStorage, TEST_PARTITION);

        createTestTable();
    }

    /** Configures a test table with columns of all supported types. */
    private void createTestTable() {
        ColumnParams pkColumn = ColumnParams.builder().name("pk").type(ColumnType.INT32).nullable(false).build();

        int schemaId = catalogId.getAndIncrement();
        int tableId = catalogId.getAndIncrement();
        int zoneId = catalogId.getAndIncrement();
        int pkIndexId = catalogId.getAndIncrement();

        List<CatalogTableColumnDescriptor> columns = Stream.concat(Stream.of(pkColumn), ALL_TYPES_COLUMN_PARAMS.stream())
                .map(CatalogUtils::fromParams)
                .collect(toList());
        IntList pkCols = IntList.of(0);
        CatalogTableDescriptor tableDescriptor = CatalogTableDescriptor.builder()
                .id(tableId)
                .schemaId(schemaId)
                .primaryKeyIndexId(pkIndexId)
                .name(TABLE_NAME)
                .zoneId(zoneId)
                .newColumns(columns)
                .primaryKeyColumns(pkCols)
                .storageProfile(CatalogService.DEFAULT_STORAGE_PROFILE)
                .build();

        when(catalog.table(eq(SCHEMA_NAME), eq(TABLE_NAME))).thenReturn(tableDescriptor);
        when(catalog.table(eq(tableId))).thenReturn(tableDescriptor);

        createCatalogIndexDescriptor(tableId, pkIndexId, PK_INDEX_NAME, true, pkColumn.type());
    }

    /**
     * Creates an IndexStorage instance using the given columns.
     *
     * @param name Index name.
     * @param built {@code True} to create a built index, {@code false} if you need to build it later.
     * @param columnTypes Column types.
     * @see #columnName(ColumnType)
     * @see #completeBuildIndex(IndexStorage)
     */
    protected abstract S createIndexStorage(String name, boolean built, ColumnType... columnTypes);

    /**
     * Creates a built IndexStorage instance using the given columns.
     *
     * @param name Index name.
     * @param columnTypes Column types.
     * @see #columnName(ColumnType)
     * @see #completeBuildIndex(IndexStorage)
     */
    protected S createIndexStorage(String name, ColumnType... columnTypes) {
        return createIndexStorage(name, true, columnTypes);
    }

    /**
     * Provides safe access to the index descriptor of the storage.
     */
    protected abstract D indexDescriptor(S index);

    abstract CatalogIndexDescriptor createCatalogIndexDescriptor(
            int tableId, int indexId, String indexName, boolean built, ColumnType... columnTypes
    );

    /**
     * Tests the {@link IndexStorage#get} method.
     */
    @Test
    public void testGet() {
        S index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.STRING);
        var serializer = new BinaryTupleRowSerializer(indexDescriptor(index));

        // First two rows have the same index key, but different row IDs.
        IndexRow row1 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));
        IndexRow row2 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));
        IndexRow row3 = serializer.serializeRow(new Object[]{ 2, "bar" }, new RowId(TEST_PARTITION));
        IndexRow row4 = serializer.serializeRow(new Object[]{ 3, "baz" }, new RowId(TEST_PARTITION));

        assertThat(getAll(index, row1), is(empty()));
        assertThat(getAll(index, row2), is(empty()));
        assertThat(getAll(index, row3), is(empty()));

        put(index, row1);
        put(index, row2);
        put(index, row3);

        assertThat(getAll(index, row1), containsInAnyOrder(row1.rowId(), row2.rowId()));
        assertThat(getAll(index, row2), containsInAnyOrder(row1.rowId(), row2.rowId()));
        assertThat(getAll(index, row3), contains(row3.rowId()));
        assertThat(getAll(index, row4), is(empty()));
    }

    /**
     * Tests that {@link IndexStorage#put} does not create row ID duplicates.
     */
    @Test
    public void testPutIdempotence() {
        S index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.STRING);
        var serializer = new BinaryTupleRowSerializer(indexDescriptor(index));

        IndexRow row = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));

        put(index, row);
        put(index, row);

        IndexRow actualRow = getSingle(index, row.indexColumns());

        assertNotNull(actualRow);

        assertThat(actualRow.rowId(), is(equalTo(row.rowId())));

        assertThat(getAll(index, row), contains(row.rowId()));
    }

    /**
     * Tests the {@link IndexStorage#remove} method.
     */
    @Test
    public void testRemove() {
        S index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.STRING);
        var serializer = new BinaryTupleRowSerializer(indexDescriptor(index));

        IndexRow row1 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));
        IndexRow row2 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));
        IndexRow row3 = serializer.serializeRow(new Object[]{ 2, "bar" }, new RowId(TEST_PARTITION));

        put(index, row1);
        put(index, row2);
        put(index, row3);

        assertThat(getAll(index, row1), containsInAnyOrder(row1.rowId(), row2.rowId()));
        assertThat(getAll(index, row2), containsInAnyOrder(row1.rowId(), row2.rowId()));
        assertThat(getAll(index, row3), contains(row3.rowId()));

        remove(index, row1);

        assertThat(getAll(index, row1), contains(row2.rowId()));
        assertThat(getAll(index, row2), contains(row2.rowId()));
        assertThat(getAll(index, row3), contains(row3.rowId()));

        remove(index, row2);

        assertThat(getAll(index, row1), is(empty()));
        assertThat(getAll(index, row2), is(empty()));
        assertThat(getAll(index, row3), contains(row3.rowId()));

        remove(index, row3);

        assertThat(getAll(index, row1), is(empty()));
        assertThat(getAll(index, row2), is(empty()));
        assertThat(getAll(index, row3), is(empty()));
    }

    /**
     * Tests that {@link IndexStorage#remove} works normally when removing a non-existent row.
     */
    @Test
    public void testRemoveIdempotence() {
        S index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.STRING);
        var serializer = new BinaryTupleRowSerializer(indexDescriptor(index));

        IndexRow row = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));

        assertDoesNotThrow(() -> remove(index, row));

        put(index, row);

        remove(index, row);

        assertThat(getAll(index, row), is(empty()));

        assertDoesNotThrow(() -> remove(index, row));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNextRowIdToBuild(boolean pk) {
        IndexStorage indexStorage;

        if (pk) {
            indexStorage = createPkIndexStorage();

            assertNull(indexStorage.getNextRowIdToBuild());
        } else {
            indexStorage = createIndexStorage(INDEX_NAME, false, ColumnType.INT32);

            assertEquals(initialRowIdToBuild(TEST_PARTITION), indexStorage.getNextRowIdToBuild());
        }

        var newNextRowIdToBuild = new RowId(TEST_PARTITION);

        partitionStorage.runConsistently(locker -> {
            indexStorage.setNextRowIdToBuild(newNextRowIdToBuild);

            return null;
        });

        assertEquals(newNextRowIdToBuild, newNextRowIdToBuild);
    }

    @Test
    void testInitialRowIdToBuildWithBuildIndex() {
        IndexStorage indexStorage = createIndexStorage(INDEX_NAME, true, ColumnType.INT32);

        assertThat(indexStorage.getNextRowIdToBuild(), is(nullValue()));
    }

    @Test
    void testInitialRowIdToBuildWithNotBuiltIndex() {
        IndexStorage indexStorage = createIndexStorage(INDEX_NAME, false, ColumnType.INT32);

        assertThat(indexStorage.getNextRowIdToBuild(), is(initialRowIdToBuild(TEST_PARTITION)));
    }

    @Test
    void testGetFromPkIndex() {
        S pkIndex = createPkIndexStorage();
        var serializer = new BinaryTupleRowSerializer(indexDescriptor(pkIndex));

        IndexRow indexRow = createIndexRow(serializer, 1);

        assertDoesNotThrow(() -> getAll(pkIndex, indexRow));
    }

    @Test
    void testGetAfterBuiltIndex() {
        S index = createIndexStorage(INDEX_NAME, false, ColumnType.INT32);
        var serializer = new BinaryTupleRowSerializer(indexDescriptor(index));

        IndexRow indexRow = createIndexRow(serializer, 1);

        assertThrows(IndexNotBuiltException.class, () -> getAll(index, indexRow));

        completeBuildIndex(index);

        assertDoesNotThrow(() -> getAll(index, indexRow));
    }

    protected static Collection<RowId> getAll(IndexStorage index, IndexRow row) {
        try (Cursor<RowId> cursor = index.get(row.indexColumns())) {
            return cursor.stream().collect(toList());
        }
    }

    /**
     * Extracts a single value by a given key or {@code null} if it does not exist.
     */
    protected static @Nullable IndexRow getSingle(IndexStorage indexStorage, BinaryTuple fullPrefix) {
        List<RowId> rowIds = get(indexStorage, fullPrefix);

        assertThat(rowIds, anyOf(empty(), hasSize(1)));

        return rowIds.isEmpty() ? null : new IndexRowImpl(fullPrefix, rowIds.get(0));
    }

    protected static List<RowId> get(IndexStorage index, BinaryTuple key) {
        try (Cursor<RowId> cursor = index.get(key)) {
            return cursor.stream().collect(toUnmodifiableList());
        }
    }

    protected final void put(S indexStorage, IndexRow row) {
        partitionStorage.runConsistently(locker -> {
            locker.lock(row.rowId());

            indexStorage.put(row);

            return null;
        });
    }

    protected final void remove(S indexStorage, IndexRow row) {
        partitionStorage.runConsistently(locker -> {
            locker.lock(row.rowId());

            indexStorage.remove(row);

            return null;
        });
    }

    private static ColumnParams.Builder columnParamsBuilder(ColumnType columnType) {
        return ColumnParams.builder().name(columnName(columnType)).type(columnType);
    }

    void addToCatalog(CatalogIndexDescriptor indexDescriptor) {
        when(catalog.aliveIndex(eq(SCHEMA_NAME), eq(indexDescriptor.name()))).thenReturn(indexDescriptor);
        when(catalog.index(eq(indexDescriptor.id()))).thenReturn(indexDescriptor);
    }

    S createPkIndexStorage() {
        CatalogTableDescriptor tableDescriptor = catalog.table(SCHEMA_NAME, TABLE_NAME);
        assertThat(tableDescriptor, is(notNullValue()));

        CatalogIndexDescriptor pkIndexDescriptor = catalog.aliveIndex(SCHEMA_NAME, PK_INDEX_NAME);
        assertThat(pkIndexDescriptor, is(notNullValue()));

        if (pkIndexDescriptor.indexType() == CatalogIndexDescriptorType.HASH) {
            tableStorage.createHashIndex(
                    TEST_PARTITION,
                    (StorageHashIndexDescriptor) StorageIndexDescriptor.create(tableDescriptor, pkIndexDescriptor)
            );
        } else {
            tableStorage.createSortedIndex(
                    TEST_PARTITION,
                    (StorageSortedIndexDescriptor) StorageIndexDescriptor.create(tableDescriptor, pkIndexDescriptor)
            );
        }

        S index = (S) tableStorage.getIndex(TEST_PARTITION, pkIndexDescriptor.id());

        assertThat(index, is(notNullValue()));

        return index;
    }

    /** Completes the building of the index and makes read operations available from it. */
    void completeBuildIndex(IndexStorage indexStorage) {
        TestStorageUtils.completeBuiltIndexes(partitionStorage, indexStorage);
    }

    private static IndexRow createIndexRow(BinaryTupleRowSerializer serializer, Object... values) {
        return serializer.serializeRow(values, new RowId(TEST_PARTITION));
    }
}
