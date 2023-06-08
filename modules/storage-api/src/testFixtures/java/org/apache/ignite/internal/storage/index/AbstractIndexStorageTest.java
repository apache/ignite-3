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
import static org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders.column;
import static org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders.tableBuilder;
import static org.apache.ignite.internal.storage.BaseMvStoragesTest.getOrCreateMvPartition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.testutils.definition.ColumnDefinition;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.impl.BinaryTupleRowSerializer;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Base class for index storage tests. Covers common methods, such as {@link IndexStorage#get(BinaryTuple)} or
 * {@link IndexStorage#put(IndexRow)}.
 *
 * @param <S> Type of specific index implementation.
 * @param <D> Type of index descriptor for that specific implementation.
 */
public abstract class AbstractIndexStorageTest<S extends IndexStorage, D extends StorageIndexDescriptor> {
    private static final IgniteLogger log = Loggers.forClass(AbstractIndexStorageTest.class);

    /** Definitions of all supported column types. */
    @SuppressWarnings("WeakerAccess") // May be used in "@VariableSource", that's why it's public.
    public static final List<ColumnDefinition> ALL_TYPES_COLUMN_DEFINITIONS = allTypesColumnDefinitions();

    protected static final int TEST_PARTITION = 0;

    protected static final String INDEX_NAME = "TEST_IDX";

    // Short name is convenient for initialization in @InjectConfiguration.
    protected static final String TABLE_NAME = "foo";

    private static List<ColumnDefinition> allTypesColumnDefinitions() {
        Stream<ColumnType> allColumnTypes = Stream.of(
                ColumnType.INT8,
                ColumnType.INT16,
                ColumnType.INT32,
                ColumnType.INT64,
                ColumnType.FLOAT,
                ColumnType.DOUBLE,
                ColumnType.UUID,
                ColumnType.DATE,
                ColumnType.bitmaskOf(32),
                ColumnType.string(),
                ColumnType.blob(),
                ColumnType.number(),
                ColumnType.decimal(),
                ColumnType.time(),
                ColumnType.datetime(),
                ColumnType.timestamp()
        );

        return allColumnTypes
                .map(type -> column(columnName(type), type).asNullable(true).build())
                .collect(toUnmodifiableList());
    }

    private final long seed = System.currentTimeMillis();

    protected final Random random = new Random(seed);

    protected MvTableStorage tableStorage;

    protected MvPartitionStorage partitionStorage;

    protected TablesConfiguration tablesCfg;

    @BeforeEach
    void setUp() {
        log.info("Using random seed: " + seed);
    }

    /**
     * Returns a name of the column with given type.
     */
    protected static String columnName(ColumnType type) {
        return type.typeSpec().name();
    }

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    protected final void initialize(MvTableStorage tableStorage, TablesConfiguration tablesCfg) {
        this.tablesCfg = tablesCfg;
        this.tableStorage = tableStorage;
        this.partitionStorage = getOrCreateMvPartition(tableStorage, TEST_PARTITION);

        createTestTable(tableStorage.configuration());
    }

    /**
     * Configures a test table with columns of all supported types.
     */
    private static void createTestTable(TableConfiguration tableCfg) {
        ColumnDefinition pkColumn = column("pk", ColumnType.INT32).asNullable(false).build();

        ColumnDefinition[] allColumns = Stream.concat(Stream.of(pkColumn), ALL_TYPES_COLUMN_DEFINITIONS.stream())
                .toArray(ColumnDefinition[]::new);

        TableDefinition tableDefinition = tableBuilder("test", TABLE_NAME)
                .columns(allColumns)
                .withPrimaryKey(pkColumn.name())
                .build();

        CompletableFuture<Void> createTableFuture = tableCfg.change(cfg -> convert(tableDefinition, cfg));

        assertThat(createTableFuture, willCompleteSuccessfully());
    }

    /**
     * Creates an IndexStorage instance using the given columns.
     *
     * @see #columnName(ColumnType)
     */
    protected abstract S createIndexStorage(String name, ColumnType... columnTypes);

    /**
     * Provides safe access to the index descriptor of the storage.
     */
    protected abstract D indexDescriptor(S index);

    /**
     * Tests the {@link IndexStorage#get} method.
     */
    @Test
    public void testGet() {
        S index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.string());
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

    @Test
    public void testGetConcurrentPut() {
        S index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.string());
        var serializer = new BinaryTupleRowSerializer(indexDescriptor(index));

        Object[] columnValues = { 1, "foo" };
        IndexRow row1 = serializer.serializeRow(columnValues, new RowId(TEST_PARTITION, 1, 1));
        IndexRow row2 = serializer.serializeRow(columnValues, new RowId(TEST_PARTITION, 2, 2));

        try (Cursor<RowId> cursor = index.get(row1.indexColumns())) {
            put(index, row1);

            assertTrue(cursor.hasNext());
            assertEquals(row1.rowId(), cursor.next());

            put(index, row2);

            assertTrue(cursor.hasNext());
            assertEquals(row2.rowId(), cursor.next());

            assertFalse(cursor.hasNext());
            assertThrows(NoSuchElementException.class, cursor::next);
        }
    }

    @Test
    public void testGetConcurrentReplace() {
        S index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.string());
        var serializer = new BinaryTupleRowSerializer(indexDescriptor(index));

        Object[] columnValues = { 1, "foo" };
        IndexRow row1 = serializer.serializeRow(columnValues, new RowId(TEST_PARTITION, 1, 1));
        IndexRow row2 = serializer.serializeRow(columnValues, new RowId(TEST_PARTITION, 2, 2));

        put(index, row1);

        try (Cursor<RowId> cursor = index.get(row1.indexColumns())) {
            assertTrue(cursor.hasNext());
            assertEquals(row1.rowId(), cursor.next());

            remove(index, row1);
            put(index, row2);

            assertTrue(cursor.hasNext());
            assertEquals(row2.rowId(), cursor.next());

            assertFalse(cursor.hasNext());
            assertThrows(NoSuchElementException.class, cursor::next);
        }
    }

    /**
     * Tests that {@link IndexStorage#put} does not create row ID duplicates.
     */
    @Test
    public void testPutIdempotence() {
        S index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.string());
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
        S index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.string());
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
        S index = createIndexStorage(INDEX_NAME, ColumnType.INT32, ColumnType.string());
        var serializer = new BinaryTupleRowSerializer(indexDescriptor(index));

        IndexRow row = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));

        assertDoesNotThrow(() -> remove(index, row));

        put(index, row);

        remove(index, row);

        assertThat(getAll(index, row), is(empty()));

        assertDoesNotThrow(() -> remove(index, row));
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
}
