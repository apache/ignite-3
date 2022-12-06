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
import static org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter.addIndex;
import static org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders.column;
import static org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders.tableBuilder;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER_OR_EQUAL;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS_OR_EQUAL;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.builder.SortedIndexDefinitionBuilder;
import org.apache.ignite.internal.schema.testutils.builder.SortedIndexDefinitionBuilder.SortedIndexColumnBuilder;
import org.apache.ignite.internal.schema.testutils.definition.ColumnDefinition;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType.ColumnTypeSpec;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.schema.testutils.definition.index.ColumnarIndexDefinition;
import org.apache.ignite.internal.schema.testutils.definition.index.SortedIndexDefinition;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.SortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.BinaryTupleRowSerializer;
import org.apache.ignite.internal.storage.index.impl.TestIndexRow;
import org.apache.ignite.internal.testframework.VariableSource;
import org.apache.ignite.internal.util.Cursor;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

/**
 * Base class for Sorted Index storage tests.
 */
public abstract class AbstractSortedIndexStorageTest {
    private static final IgniteLogger log = Loggers.forClass(AbstractSortedIndexStorageTest.class);

    /** Definitions of all supported column types. */
    public static final List<ColumnDefinition> ALL_TYPES_COLUMN_DEFINITIONS = allTypesColumnDefinitions();

    protected static final int TEST_PARTITION = 0;

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
                .map(type -> column(type.typeSpec().name(), type).asNullable(true).build())
                .collect(toUnmodifiableList());
    }

    private final Random random;

    private MvTableStorage tableStorage;

    private MvPartitionStorage partitionStorage;

    private TablesConfiguration tablesCfg;

    protected AbstractSortedIndexStorageTest() {
        long seed = System.currentTimeMillis();

        log.info("Using random seed: " + seed);

        random = new Random(seed);
    }

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    protected final void initialize(MvTableStorage tableStorage, TablesConfiguration tablesCfg) {
        this.tablesCfg = tablesCfg;
        this.tableStorage = tableStorage;
        this.partitionStorage = tableStorage.getOrCreateMvPartition(TEST_PARTITION);

        createTestTable(tableStorage.configuration());
    }

    /**
     * Configures a test table with columns of all supported types.
     */
    private static void createTestTable(TableConfiguration tableCfg) {
        ColumnDefinition pkColumn = column("pk", ColumnType.INT32).asNullable(false).build();

        ColumnDefinition[] allColumns = Stream.concat(Stream.of(pkColumn), ALL_TYPES_COLUMN_DEFINITIONS.stream())
                .toArray(ColumnDefinition[]::new);

        TableDefinition tableDefinition = tableBuilder("test", "foo")
                .columns(allColumns)
                .withPrimaryKey(pkColumn.name())
                .build();

        CompletableFuture<Void> createTableFuture = tableCfg.change(cfg -> convert(tableDefinition, cfg));

        assertThat(createTableFuture, willCompleteSuccessfully());
    }

    /**
     * Creates a Sorted Index using the given columns.
     */
    private SortedIndexStorage createIndexStorage(List<ColumnDefinition> indexSchema) {
        SortedIndexDefinitionBuilder indexDefinitionBuilder = SchemaBuilders.sortedIndex("TEST_IDX");

        indexSchema.forEach(column -> {
            SortedIndexColumnBuilder columnBuilder = indexDefinitionBuilder.addIndexColumn(column.name());

            if (random.nextBoolean()) {
                columnBuilder.asc();
            } else {
                columnBuilder.desc();
            }

            columnBuilder.done();
        });

        SortedIndexDefinition indexDefinition = indexDefinitionBuilder.build();

        return createIndexStorage(indexDefinition);
    }

    /**
     * Creates a Sorted Index using the given index definition.
     */
    protected SortedIndexStorage createIndexStorage(ColumnarIndexDefinition indexDefinition) {
        CompletableFuture<Void> createIndexFuture =
                tablesCfg.indexes().change(chg -> chg.create(indexDefinition.name(), idx -> {
                    UUID tableId = ConfigurationUtil.internalId(tablesCfg.tables().value(), "foo");

                    addIndex(indexDefinition, tableId, idx);
                }));

        assertThat(createIndexFuture, willCompleteSuccessfully());

        TableIndexView indexConfig = tablesCfg.indexes().get(indexDefinition.name()).value();

        return tableStorage.getOrCreateSortedIndex(TEST_PARTITION, indexConfig.id());
    }

    /**
     * Tests that columns of all types are correctly serialized and deserialized.
     */
    @Test
    void testRowSerialization() {
        SortedIndexStorage indexStorage = createIndexStorage(ALL_TYPES_COLUMN_DEFINITIONS);

        Object[] columns = indexStorage.indexDescriptor().columns().stream()
                .map(SortedIndexColumnDescriptor::type)
                .map(type -> SchemaTestUtils.generateRandomValue(random, type))
                .toArray();

        var serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        IndexRow row = serializer.serializeRow(columns, new RowId(TEST_PARTITION));

        Object[] actual = serializer.deserializeColumns(row);

        assertThat(actual, is(equalTo(columns)));
    }

    @Test
    void testEmpty() {
        SortedIndexStorage index = createIndexStorage(shuffledRandomDefinitions());

        assertThat(scan(index, null, null, 0), is(empty()));
    }

    @Test
    void testGet() {
        SortedIndexDefinition indexDefinition = SchemaBuilders.sortedIndex("TEST_INDEX")
                .addIndexColumn(ColumnTypeSpec.STRING.name()).asc().done()
                .addIndexColumn(ColumnTypeSpec.INT32.name()).asc().done()
                .build();

        SortedIndexStorage index = createIndexStorage(indexDefinition);

        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        IndexRow val1090 = serializer.serializeRow(new Object[]{ "10", 90 }, new RowId(TEST_PARTITION));
        IndexRow otherVal1090 = serializer.serializeRow(new Object[]{ "10", 90 }, new RowId(TEST_PARTITION));
        IndexRow val1080 = serializer.serializeRow(new Object[]{ "10", 80 }, new RowId(TEST_PARTITION));
        IndexRow val2090 = serializer.serializeRow(new Object[]{ "20", 90 }, new RowId(TEST_PARTITION));

        put(index, val1090);
        put(index, otherVal1090);
        put(index, val1080);

        assertThat(get(index, val1090.indexColumns()), containsInAnyOrder(val1090.rowId(), otherVal1090.rowId()));

        assertThat(get(index, otherVal1090.indexColumns()), containsInAnyOrder(val1090.rowId(), otherVal1090.rowId()));

        assertThat(get(index, val1080.indexColumns()), containsInAnyOrder(val1080.rowId()));

        assertThat(get(index, val2090.indexColumns()), is(empty()));
    }

    /**
     * Tests the Put-Get-Remove case when an index is created using a single column.
     */
    @ParameterizedTest
    @VariableSource("ALL_TYPES_COLUMN_DEFINITIONS")
    void testSingleColumnIndex(ColumnDefinition columnDefinition) {
        testPutGetRemove(List.of(columnDefinition));
    }

    /**
     * Tests that appending an already existing row does no harm.
     */
    @Test
    void testPutIdempotence() {
        SortedIndexDefinition indexDefinition = SchemaBuilders.sortedIndex("TEST_INDEX")
                .addIndexColumn(ColumnTypeSpec.STRING.name()).asc().done()
                .addIndexColumn(ColumnTypeSpec.INT32.name()).asc().done()
                .build();

        SortedIndexStorage index = createIndexStorage(indexDefinition);

        var columnValues = new Object[] { "foo", 1 };
        var rowId = new RowId(TEST_PARTITION);

        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        IndexRow row = serializer.serializeRow(columnValues, rowId);

        put(index, row);
        put(index, row);

        IndexRow actualRow = getSingle(index, row.indexColumns());

        assertThat(actualRow.rowId(), is(equalTo(row.rowId())));
    }

    /**
     * Tests that it is possible to add rows with the same columns but different Row IDs.
     */
    @Test
    void testMultiplePuts() {
        SortedIndexDefinition indexDefinition = SchemaBuilders.sortedIndex("TEST_INDEX")
                .addIndexColumn(ColumnTypeSpec.STRING.name()).asc().done()
                .addIndexColumn(ColumnTypeSpec.INT32.name()).asc().done()
                .build();

        SortedIndexStorage index = createIndexStorage(indexDefinition);

        var columnValues1 = new Object[] { "foo", 1 };
        var columnValues2 = new Object[] { "bar", 3 };
        var rowId1 = new RowId(TEST_PARTITION);
        var rowId2 = new RowId(TEST_PARTITION);
        var rowId3 = new RowId(TEST_PARTITION);

        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        IndexRow row1 = serializer.serializeRow(columnValues1, rowId1);
        IndexRow row2 = serializer.serializeRow(columnValues1, rowId2);
        IndexRow row3 = serializer.serializeRow(columnValues2, rowId3);

        put(index, row1);
        put(index, row2);
        put(index, row3);

        List<Object[]> actualColumns = scan(index, null, null, 0);

        assertThat(actualColumns, contains(columnValues2, columnValues1, columnValues1));
    }

    /**
     * Tests the {@link SortedIndexStorage#remove} method.
     */
    @Test
    void testRemove() {
        SortedIndexDefinition indexDefinition = SchemaBuilders.sortedIndex("TEST_INDEX")
                .addIndexColumn(ColumnTypeSpec.STRING.name()).asc().done()
                .addIndexColumn(ColumnTypeSpec.INT32.name()).asc().done()
                .build();

        SortedIndexStorage index = createIndexStorage(indexDefinition);

        var columnValues1 = new Object[] { "foo", 1 };
        var columnValues2 = new Object[] { "bar", 3 };
        var rowId1 = new RowId(TEST_PARTITION);
        var rowId2 = new RowId(TEST_PARTITION);
        var rowId3 = new RowId(TEST_PARTITION);

        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        IndexRow row1 = serializer.serializeRow(columnValues1, rowId1);
        IndexRow row2 = serializer.serializeRow(columnValues1, rowId2);
        IndexRow row3 = serializer.serializeRow(columnValues2, rowId3);

        put(index, row1);
        put(index, row2);
        put(index, row3);

        List<Object[]> actualColumns = scan(index, null, null, 0);

        assertThat(actualColumns, contains(columnValues2, columnValues1, columnValues1));

        // Test that rows with the same indexed columns can be removed individually
        remove(index, row2);

        actualColumns = scan(index, null, null, 0);

        assertThat(actualColumns, contains(columnValues2, columnValues1));

        // Test that removing a non-existent row does nothing
        remove(index, row2);

        actualColumns = scan(index, null, null, 0);

        assertThat(actualColumns, contains(columnValues2, columnValues1));

        // Test that the first row can be actually removed
        remove(index, row1);

        actualColumns = scan(index, null, null, 0);

        assertThat(actualColumns, contains((Object) columnValues2));
    }

    /**
     * Tests the Put-Get-Remove case when an index is created using all possible column in random order.
     */
    @RepeatedTest(5)
    void testCreateMultiColumnIndex() {
        testPutGetRemove(shuffledDefinitions());
    }

    /**
     * Tests the happy case of the {@link SortedIndexStorage#scan} method.
     */
    @RepeatedTest(5)
    void testScan() {
        SortedIndexStorage indexStorage = createIndexStorage(shuffledDefinitions());

        List<TestIndexRow> entries = IntStream.range(0, 10)
                .mapToObj(i -> {
                    TestIndexRow entry = TestIndexRow.randomRow(indexStorage);

                    put(indexStorage, entry);

                    return entry;
                })
                .sorted()
                .collect(toList());

        int firstIndex = 3;
        int lastIndex = 8;

        List<IndexRow> expected = entries.stream()
                .skip(firstIndex)
                .limit(lastIndex - firstIndex + 1)
                .collect(toList());

        BinaryTuplePrefix first = entries.get(firstIndex).prefix(3);
        BinaryTuplePrefix last = entries.get(lastIndex).prefix(5);

        try (Cursor<IndexRow> cursor = indexStorage.scan(first, last, GREATER_OR_EQUAL | LESS_OR_EQUAL)) {
            List<IndexRow> actual = cursor.stream().collect(toList());

            assertThat(actual, hasSize(lastIndex - firstIndex + 1));

            for (int i = firstIndex; i < actual.size(); ++i) {
                assertThat(actual.get(i).rowId(), is(equalTo(expected.get(i).rowId())));
            }
        }
    }

    @Test
    public void testBoundsAndOrder() {
        ColumnTypeSpec string = ColumnTypeSpec.STRING;
        ColumnTypeSpec int32 = ColumnTypeSpec.INT32;

        SortedIndexDefinition index1Definition = SchemaBuilders.sortedIndex("TEST_INDEX_1")
                .addIndexColumn(string.name()).asc().done()
                .addIndexColumn(int32.name()).asc().done()
                .build();

        SortedIndexDefinition index2Definition = SchemaBuilders.sortedIndex("TEST_INDEX_2")
                .addIndexColumn(string.name()).asc().done()
                .addIndexColumn(int32.name()).desc().done()
                .build();

        SortedIndexStorage index1 = createIndexStorage(index1Definition);
        SortedIndexStorage index2 = createIndexStorage(index2Definition);

        Object[] val1090 = { "10", 90 };
        Object[] val1080 = { "10", 80 };
        Object[] val2090 = { "20", 90 };
        Object[] val2080 = { "20", 80 };

        for (SortedIndexStorage index : Arrays.asList(index1, index2)) {
            var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

            put(index, serializer.serializeRow(val1090, new RowId(TEST_PARTITION)));
            put(index, serializer.serializeRow(val1080, new RowId(TEST_PARTITION)));
            put(index, serializer.serializeRow(val2090, new RowId(TEST_PARTITION)));
            put(index, serializer.serializeRow(val2080, new RowId(TEST_PARTITION)));
        }

        // Test without bounds.
        assertThat(
                scan(index1, null, null, 0),
                contains(val1080, val1090, val2080, val2090)
        );

        assertThat(
                scan(index2, null, null, 0),
                contains(val1090, val1080, val2090, val2080)
        );

        // Lower bound exclusive.
        assertThat(
                scan(index1, prefix(index1, "10"), null, GREATER),
                contains(val2080, val2090)
        );

        assertThat(
                scan(index2, prefix(index2, "10"), null, GREATER),
                contains(val2090, val2080)
        );

        // Lower bound inclusive.
        assertThat(
                scan(index1, prefix(index1, "10"), null, GREATER_OR_EQUAL),
                contains(val1080, val1090, val2080, val2090)
        );

        assertThat(
                scan(index2, prefix(index2, "10"), null, GREATER_OR_EQUAL),
                contains(val1090, val1080, val2090, val2080)
        );

        // Upper bound exclusive.
        assertThat(
                scan(index1, null, prefix(index1, "20"), LESS),
                contains(val1080, val1090)
        );

        assertThat(
                scan(index2, null, prefix(index2, "20"), LESS),
                contains(val1090, val1080)
        );

        // Upper bound inclusive.
        assertThat(
                scan(index1, null, prefix(index1, "20"), LESS_OR_EQUAL),
                contains(val1080, val1090, val2080, val2090)
        );

        assertThat(
                scan(index2, null, prefix(index2, "20"), LESS_OR_EQUAL),
                contains(val1090, val1080, val2090, val2080)
        );
    }

    /**
     * Tests that an empty range is returned if {@link SortedIndexStorage#scan} method is called using overlapping keys.
     */
    @Test
    void testEmptyRange() {
        List<ColumnDefinition> indexSchema = shuffledRandomDefinitions();

        SortedIndexStorage indexStorage = createIndexStorage(indexSchema);

        TestIndexRow entry1 = TestIndexRow.randomRow(indexStorage);
        TestIndexRow entry2 = TestIndexRow.randomRow(indexStorage);

        if (entry2.compareTo(entry1) < 0) {
            TestIndexRow t = entry2;
            entry2 = entry1;
            entry1 = t;
        }

        put(indexStorage, entry1);
        put(indexStorage, entry2);

        try (Cursor<IndexRow> cursor = indexStorage.scan(entry2.prefix(indexSchema.size()), entry1.prefix(indexSchema.size()), 0)) {
            assertThat(cursor.stream().collect(toList()), is(empty()));
        }
    }

    @ParameterizedTest
    @VariableSource("ALL_TYPES_COLUMN_DEFINITIONS")
    void testNullValues(ColumnDefinition columnDefinition) {
        SortedIndexStorage storage = createIndexStorage(List.of(columnDefinition));

        TestIndexRow entry1 = TestIndexRow.randomRow(storage);

        Object[] nullArray = new Object[1];

        var serializer = new BinaryTupleRowSerializer(storage.indexDescriptor());

        IndexRow nullRow = serializer.serializeRow(nullArray, new RowId(TEST_PARTITION));

        var entry2 = new TestIndexRow(storage, serializer, nullRow, nullArray);

        put(storage, entry1);
        put(storage, entry2);

        if (entry1.compareTo(entry2) > 0) {
            TestIndexRow t = entry2;
            entry2 = entry1;
            entry1 = t;
        }

        try (Cursor<IndexRow> cursor = storage.scan(entry1.prefix(1), entry2.prefix(1), GREATER_OR_EQUAL | LESS_OR_EQUAL)) {
            assertThat(
                    cursor.stream().map(row -> row.indexColumns().byteBuffer()).collect(toList()),
                    contains(entry1.indexColumns().byteBuffer(), entry2.indexColumns().byteBuffer())
            );
        }
    }

    /**
     * Validates the {@link SortedIndexStorage#scan(BinaryTuplePrefix, BinaryTuplePrefix, int)} contract.
     * <ul>
     *     <li>At the moment of creating the cursor, we get its first IndexRow;</li>
     *     <li>Cursor works with the current state of the storage, if the changes are within the range of the cursor, then we will receive
     *     them.</li>
     * </ul>
     */
    @Test
    void testScanContract() {
        SortedIndexDefinition indexDefinition = SchemaBuilders.sortedIndex("TEST_IDX")
                .addIndexColumn(ColumnType.INT32.typeSpec().name()).asc().done()
                .build();

        SortedIndexStorage indexStorage = createIndexStorage(indexDefinition);

        assertThat(indexStorage.scan(null, null, 0).stream().collect(toList()), empty());

        Cursor<IndexRow> scan = indexStorage.scan(null, null, 0);

        BinaryTupleRowSerializer serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        RowId rowId = new RowId(TEST_PARTITION);

        // Let's check that if we now add an IndexRow, then we will not get it by the cursor.
        put(indexStorage, serializer.serializeRow(new Object[]{0}, rowId));

        assertThat(scan.stream().collect(toList()), empty());

        // But new cursors will see this IndexRow.
        assertThat(
                indexStorage.scan(null, null, 0).stream()
                        .map(indexRow -> serializer.deserializeColumns(indexRow)[0])
                        .collect(toList()),
                contains(0)
        );

        // Let's check that the added IndexRows to the storage will be visible for the cursor.
        scan = indexStorage.scan(null, null, 0);

        assertTrue(scan.hasNext());

        // Let's add an IndexRow after method Cursor#hasNext.
        put(indexStorage, serializer.serializeRow(new Object[]{1}, rowId));

        assertEquals(0, serializer.deserializeColumns(scan.next())[0]);

        assertTrue(scan.hasNext());

        // Let's add an IndexRow before method Cursor#hasNext.
        put(indexStorage, serializer.serializeRow(new Object[]{2}, rowId));

        assertTrue(scan.hasNext());

        assertEquals(1, serializer.deserializeColumns(scan.next())[0]);

        assertTrue(scan.hasNext());

        assertEquals(2, serializer.deserializeColumns(scan.next())[0]);

        assertFalse(scan.hasNext());

        assertThrows(NoSuchElementException.class, scan::next);

        // Let's check that the removed IndexRow's from the storage will be visible for the cursor.
        scan = indexStorage.scan(null, null, 0);

        // Let's check that if we now remove an IndexRow, then we will get it by the cursor.
        remove(indexStorage, serializer.serializeRow(new Object[]{0}, rowId));

        // Let's remove an IndexRows before method Cursor#hasNext.
        remove(indexStorage, serializer.serializeRow(new Object[]{1}, rowId));

        assertTrue(scan.hasNext());

        // Let's remove an IndexRows after method Cursor#hasNext.
        remove(indexStorage, serializer.serializeRow(new Object[]{2}, rowId));

        assertEquals(0, serializer.deserializeColumns(scan.next())[0]);

        assertFalse(scan.hasNext());

        assertThrows(NoSuchElementException.class, scan::next);
    }

    private List<ColumnDefinition> shuffledRandomDefinitions() {
        return shuffledDefinitions(d -> random.nextBoolean());
    }

    private List<ColumnDefinition> shuffledDefinitions() {
        return shuffledDefinitions(d -> true);
    }

    private List<ColumnDefinition> shuffledDefinitions(Predicate<ColumnDefinition> filter) {
        List<ColumnDefinition> shuffledDefinitions = ALL_TYPES_COLUMN_DEFINITIONS.stream()
                .filter(filter)
                .collect(toList());

        if (shuffledDefinitions.isEmpty()) {
            shuffledDefinitions = new ArrayList<>(ALL_TYPES_COLUMN_DEFINITIONS);
        }

        Collections.shuffle(shuffledDefinitions, random);

        if (log.isInfoEnabled()) {
            List<String> columnNames = shuffledDefinitions.stream().map(ColumnDefinition::name).collect(toList());

            log.info("Creating index with the following column order: " + columnNames);
        }

        return shuffledDefinitions;
    }

    /**
     * Tests the Get-Put-Remove scenario: inserts some keys into the storage and checks that they have been successfully persisted and can
     * be removed.
     */
    private void testPutGetRemove(List<ColumnDefinition> indexSchema) {
        SortedIndexStorage indexStorage = createIndexStorage(indexSchema);

        TestIndexRow entry1 = TestIndexRow.randomRow(indexStorage);
        TestIndexRow entry2;

        // using a cycle here to protect against equal keys being generated
        do {
            entry2 = TestIndexRow.randomRow(indexStorage);
        } while (entry1.indexColumns().byteBuffer().equals(entry2.indexColumns().byteBuffer()));

        put(indexStorage, entry1);
        put(indexStorage, entry2);

        assertThat(
                getSingle(indexStorage, entry1.indexColumns()).rowId(),
                is(equalTo(entry1.rowId()))
        );

        assertThat(
                getSingle(indexStorage, entry2.indexColumns()).rowId(),
                is(equalTo(entry2.rowId()))
        );

        remove(indexStorage, entry1);

        assertThat(getSingle(indexStorage, entry1.indexColumns()), is(nullValue()));
    }

    /**
     * Extracts a single value by a given key or {@code null} if it does not exist.
     */
    @Nullable
    private static IndexRow getSingle(SortedIndexStorage indexStorage, BinaryTuple fullPrefix) {
        List<RowId> rowIds = get(indexStorage, fullPrefix);

        assertThat(rowIds, anyOf(empty(), hasSize(1)));

        return rowIds.isEmpty() ? null : new IndexRowImpl(fullPrefix, rowIds.get(0));
    }

    private static BinaryTuplePrefix prefix(SortedIndexStorage index, Object... vals) {
        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        return serializer.serializeRowPrefix(vals);
    }

    private static List<Object[]> scan(
            SortedIndexStorage index,
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            @MagicConstant(flagsFromClass = SortedIndexStorage.class) int flags
    ) {
        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        try (Cursor<IndexRow> cursor = index.scan(lowerBound, upperBound, flags)) {
            return cursor.stream()
                    .map(serializer::deserializeColumns)
                    .collect(toUnmodifiableList());
        }
    }

    protected static List<RowId> get(SortedIndexStorage index, BinaryTuple key) {
        try (Cursor<RowId> cursor = index.get(key)) {
            return cursor.stream().collect(toUnmodifiableList());
        }
    }

    protected void put(SortedIndexStorage indexStorage, IndexRow row) {
        partitionStorage.runConsistently(() -> {
            indexStorage.put(row);

            return null;
        });
    }

    private void remove(SortedIndexStorage indexStorage, IndexRow row) {
        partitionStorage.runConsistently(() -> {
            indexStorage.remove(row);

            return null;
        });
    }

    private static <T> List<T> getRemaining(Cursor<IndexRow> scanCursor, Function<IndexRow, T> mapper) {
        List<T> result = new ArrayList<>();

        while (scanCursor.hasNext()) {
            result.add(mapper.apply(scanCursor.next()));
        }

        return result;
    }

    private static <T> Function<IndexRow, T> firstColumn(BinaryTupleRowSerializer serializer) {
        return indexRow -> (T) serializer.deserializeColumns(indexRow)[0];
    }
}
