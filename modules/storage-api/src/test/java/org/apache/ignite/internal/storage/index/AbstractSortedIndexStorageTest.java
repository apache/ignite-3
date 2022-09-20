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
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders.column;
import static org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders.tableBuilder;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER_OR_EQUAL;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS_OR_EQUAL;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomString;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.builder.SortedIndexDefinitionBuilder;
import org.apache.ignite.internal.schema.testutils.builder.SortedIndexDefinitionBuilder.SortedIndexColumnBuilder;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.ColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.BinaryTupleRowSerializer;
import org.apache.ignite.internal.storage.index.impl.TestIndexRow;
import org.apache.ignite.internal.testframework.VariableSource;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.index.ColumnarIndexDefinition;
import org.apache.ignite.schema.definition.index.SortedIndexDefinition;
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

    private static final int TEST_PARTITION = 0;

    private Random random;

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
        SortedIndexDefinitionBuilder indexDefinitionBuilder = SchemaBuilders.sortedIndex(randomString(random, 10));

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
    private SortedIndexStorage createIndexStorage(ColumnarIndexDefinition indexDefinition) {
        CompletableFuture<Void> createIndexFuture =
                tablesCfg.indexes().change(chg -> chg.create(indexDefinition.name(), idx -> convert(indexDefinition, idx)));

        assertThat(createIndexFuture, willBe(nullValue(Void.class)));

        TableIndexView indexConfig = tablesCfg.indexes().get(indexDefinition.name()).value();

        return tableStorage.getOrCreateSortedIndex(0, indexConfig.id());
    }

    /**
     * Tests that columns of all types are correctly serialized and deserialized.
     */
    @Test
    void testRowSerialization() {
        SortedIndexStorage indexStorage = createIndexStorage(ALL_TYPES_COLUMN_DEFINITIONS);

        Object[] columns = indexStorage.indexDescriptor().indexColumns().stream()
                .map(ColumnDescriptor::type)
                .map(type -> SchemaTestUtils.generateRandomValue(random, type))
                .toArray();

        var serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());

        IndexRow row = serializer.serializeRow(columns, new RowId(0));

        Object[] actual = serializer.deserializeColumns(row);

        assertThat(actual, is(equalTo(columns)));
    }

    @Test
    public void testEmpty() throws Exception {
        SortedIndexStorage index = createIndexStorage(shuffledRandomDefinitions());

        assertThat(scan(index, null, null, 0), is(empty()));
    }

    /**
     * Tests the Put-Get-Remove case when an index is created using a single column.
     */
    @ParameterizedTest
    @VariableSource("ALL_TYPES_COLUMN_DEFINITIONS")
    void testSingleColumnIndex(ColumnDefinition columnDefinition) throws Exception {
        testPutGetRemove(List.of(columnDefinition));
    }

    /**
     * Tests that appending an already existing row does no harm.
     */
    @Test
    void testPutIdempotence() throws Exception {
        List<ColumnDefinition> columns = List.of(
                column(ColumnType.string().typeSpec().name(), ColumnType.string()).asNullable(false).build(),
                column(ColumnType.INT32.typeSpec().name(), ColumnType.string()).asNullable(false).build()
        );

        SortedIndexDefinition indexDefinition = SchemaBuilders.sortedIndex(randomString(random, 10))
                .addIndexColumn(columns.get(0).name()).asc().done()
                .addIndexColumn(columns.get(1).name()).asc().done()
                .build();

        SortedIndexStorage index = createIndexStorage(indexDefinition);

        var columnValues = new Object[] { "foo", 1 };
        var rowId = new RowId(0);

        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        IndexRow row = serializer.serializeRow(columnValues, rowId);

        put(index, row);
        put(index, row);

        IndexRow actualRow = getSingle(index, serializer.serializeRowPrefix(columnValues));

        assertThat(actualRow.rowId(), is(equalTo(row.rowId())));
    }

    /**
     * Tests that it is possible to add rows with the same columns but different Row IDs.
     */
    @Test
    void testMultiplePuts() throws Exception {
        List<ColumnDefinition> columns = List.of(
                column(ColumnType.string().typeSpec().name(), ColumnType.string()).asNullable(false).build(),
                column(ColumnType.INT32.typeSpec().name(), ColumnType.string()).asNullable(false).build()
        );

        SortedIndexDefinition indexDefinition = SchemaBuilders.sortedIndex(randomString(random, 10))
                .addIndexColumn(columns.get(0).name()).asc().done()
                .addIndexColumn(columns.get(1).name()).asc().done()
                .build();

        SortedIndexStorage index = createIndexStorage(indexDefinition);

        var columnValues1 = new Object[] { "foo", 1 };
        var columnValues2 = new Object[] { "bar", 3 };
        var rowId1 = new RowId(0);
        var rowId2 = new RowId(0);
        var rowId3 = new RowId(0);

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
    void testRemove() throws Exception {
        List<ColumnDefinition> columns = List.of(
                column(ColumnType.string().typeSpec().name(), ColumnType.string()).asNullable(false).build(),
                column(ColumnType.INT32.typeSpec().name(), ColumnType.string()).asNullable(false).build()
        );

        SortedIndexDefinition indexDefinition = SchemaBuilders.sortedIndex(randomString(random, 10))
                .addIndexColumn(columns.get(0).name()).asc().done()
                .addIndexColumn(columns.get(1).name()).asc().done()
                .build();

        SortedIndexStorage index = createIndexStorage(indexDefinition);

        var columnValues1 = new Object[] { "foo", 1 };
        var columnValues2 = new Object[] { "bar", 3 };
        var rowId1 = new RowId(0);
        var rowId2 = new RowId(0);
        var rowId3 = new RowId(0);

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
    void testCreateMultiColumnIndex() throws Exception {
        testPutGetRemove(shuffledDefinitions());
    }

    /**
     * Tests the happy case of the {@link SortedIndexStorage#scan} method.
     */
    @RepeatedTest(5)
    void testScan() throws Exception {
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
    public void testBoundsAndOrder() throws Exception {
        List<ColumnDefinition> columns = List.of(
                column(ColumnType.string().typeSpec().name(), ColumnType.string()).asNullable(false).build(),
                column(ColumnType.INT32.typeSpec().name(), ColumnType.string()).asNullable(false).build()
        );

        SortedIndexDefinition index1Definition = SchemaBuilders.sortedIndex(randomString(random, 10))
                .addIndexColumn(columns.get(0).name()).asc().done()
                .addIndexColumn(columns.get(1).name()).asc().done()
                .build();

        SortedIndexDefinition index2Definition = SchemaBuilders.sortedIndex(randomString(random, 10))
                .addIndexColumn(columns.get(0).name()).asc().done()
                .addIndexColumn(columns.get(1).name()).desc().done()
                .build();

        SortedIndexStorage index1 = createIndexStorage(index1Definition);
        SortedIndexStorage index2 = createIndexStorage(index2Definition);

        Object[] val9010 = { "10", 90 };
        Object[] val8010 = { "10", 80 };
        Object[] val9020 = { "20", 90 };
        Object[] val8020 = { "20", 80 };

        for (SortedIndexStorage index : Arrays.asList(index1, index2)) {
            var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

            put(index, serializer.serializeRow(val9010, new RowId(0)));
            put(index, serializer.serializeRow(val8010, new RowId(0)));
            put(index, serializer.serializeRow(val9020, new RowId(0)));
            put(index, serializer.serializeRow(val8020, new RowId(0)));
        }

        // Test without bounds.
        assertThat(
                scan(index1, null, null, 0),
                contains(val8010, val9010, val8020, val9020)
        );

        assertThat(
                scan(index2, null, null, 0),
                contains(val9010, val8010, val9020, val8020)
        );

        // Lower bound exclusive.
        assertThat(
                scan(index1, prefix(index1, "10"), null, GREATER),
                contains(val8020, val9020)
        );

        assertThat(
                scan(index2, prefix(index2, "10"), null, GREATER),
                contains(val9020, val8020)
        );

        // Lower bound inclusive.
        assertThat(
                scan(index1, prefix(index1, "10"), null, GREATER_OR_EQUAL),
                contains(val8010, val9010, val8020, val9020)
        );

        assertThat(
                scan(index2, prefix(index2, "10"), null, GREATER_OR_EQUAL),
                contains(val9010, val8010, val9020, val8020)
        );

        // Upper bound exclusive.
        assertThat(
                scan(index1, null, prefix(index1, "20"), LESS),
                contains(val8010, val9010)
        );

        assertThat(
                scan(index2, null, prefix(index2, "20"), LESS),
                contains(val9010, val8010)
        );

        // Upper bound inclusive.
        assertThat(
                scan(index1, null, prefix(index1, "20"), LESS_OR_EQUAL),
                contains(val8010, val9010, val8020, val9020)
        );

        assertThat(
                scan(index2, null, prefix(index2, "20"), LESS_OR_EQUAL),
                contains(val9010, val8010, val9020, val8020)
        );
    }

    /**
     * Tests that an empty range is returned if {@link SortedIndexStorage#scan} method is called using overlapping keys.
     */
    @Test
    void testEmptyRange() throws Exception {
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
    void testNullValues(ColumnDefinition columnDefinition) throws Exception {
        SortedIndexStorage storage = createIndexStorage(List.of(columnDefinition));

        TestIndexRow entry1 = TestIndexRow.randomRow(storage);

        Object[] nullArray = new Object[1];

        var serializer = new BinaryTupleRowSerializer(storage.indexDescriptor());

        IndexRow nullRow = serializer.serializeRow(nullArray, new RowId(0));

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
    private void testPutGetRemove(List<ColumnDefinition> indexSchema) throws Exception {
        SortedIndexStorage indexStorage = createIndexStorage(indexSchema);

        TestIndexRow entry1 = TestIndexRow.randomRow(indexStorage);
        TestIndexRow entry2;

        // using a cycle here to protect against equal keys being generated
        do {
            entry2 = TestIndexRow.randomRow(indexStorage);
        } while (entry1.equals(entry2));

        put(indexStorage, entry1);
        put(indexStorage, entry2);

        assertThat(
                getSingle(indexStorage, entry1.prefix(indexSchema.size())).rowId(),
                is(equalTo(entry1.rowId()))
        );

        assertThat(
                getSingle(indexStorage, entry2.prefix(indexSchema.size())).rowId(),
                is(equalTo(entry2.rowId()))
        );

        remove(indexStorage, entry1);

        assertThat(getSingle(indexStorage, entry1.prefix(indexSchema.size())), is(nullValue()));
    }

    /**
     * Extracts a single value by a given key or {@code null} if it does not exist.
     */
    @Nullable
    private static IndexRow getSingle(SortedIndexStorage indexStorage, BinaryTuplePrefix fullPrefix) throws Exception {
        try (Cursor<IndexRow> cursor = indexStorage.scan(fullPrefix, fullPrefix, GREATER_OR_EQUAL | LESS_OR_EQUAL)) {
            List<IndexRow> values = cursor.stream().collect(toList());

            assertThat(values, anyOf(empty(), hasSize(1)));

            return values.isEmpty() ? null : values.get(0);
        }
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
    ) throws Exception {
        var serializer = new BinaryTupleRowSerializer(index.indexDescriptor());

        try (Cursor<IndexRow> cursor = index.scan(lowerBound, upperBound, flags)) {
            return cursor.stream()
                    .map(serializer::deserializeColumns)
                    .collect(toUnmodifiableList());
        }
    }

    private void put(SortedIndexStorage indexStorage, IndexRow row) {
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
}
