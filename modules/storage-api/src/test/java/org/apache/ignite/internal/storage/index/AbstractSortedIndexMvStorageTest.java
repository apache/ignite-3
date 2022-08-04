/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.apache.ignite.internal.storage.index.SortedIndexMvStorage.BACKWARDS;
import static org.apache.ignite.internal.storage.index.SortedIndexMvStorage.FORWARD;
import static org.apache.ignite.internal.storage.index.SortedIndexMvStorage.GREATER;
import static org.apache.ignite.internal.storage.index.SortedIndexMvStorage.GREATER_OR_EQUAL;
import static org.apache.ignite.internal.storage.index.SortedIndexMvStorage.LESS;
import static org.apache.ignite.internal.storage.index.SortedIndexMvStorage.LESS_OR_EQUAL;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomString;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.schema.SchemaBuilders.column;
import static org.apache.ignite.schema.SchemaBuilders.tableBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.storage.chm.TestConcurrentHashMapStorageEngine;
import org.apache.ignite.internal.storage.chm.schema.TestConcurrentHashMapDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.impl.TestRowId;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.ColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestIndexRow;
import org.apache.ignite.internal.testframework.VariableSource;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.builder.SortedIndexDefinitionBuilder;
import org.apache.ignite.schema.definition.builder.SortedIndexDefinitionBuilder.SortedIndexColumnBuilder;
import org.apache.ignite.schema.definition.index.ColumnarIndexDefinition;
import org.apache.ignite.schema.definition.index.SortedIndexDefinition;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;

/**
 * Base test for MV index storages.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class AbstractSortedIndexMvStorageTest {
    private static final IgniteLogger log = Loggers.forClass(AbstractSortedIndexMvStorageTest.class);

    /** Definitions of all supported column types. */
    public static final List<ColumnDefinition> ALL_TYPES_COLUMN_DEFINITIONS = allTypesColumnDefinitions();

    protected TableConfiguration tableCfg;

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

    @BeforeEach
    void setUp(@InjectConfiguration(
            polymorphicExtensions = {
                    SortedIndexConfigurationSchema.class,
                    TestConcurrentHashMapDataStorageConfigurationSchema.class,
                    ConstantValueDefaultConfigurationSchema.class,
                    FunctionCallDefaultConfigurationSchema.class,
                    NullValueDefaultConfigurationSchema.class
            },
            // This value only required for configuration validity, it's not used otherwise.
            value = "mock.dataStorage.name = " + TestConcurrentHashMapStorageEngine.ENGINE_NAME
    ) TableConfiguration tableCfg) {
        createTestTable(tableCfg);

        this.tableCfg = tableCfg;

        long seed = System.currentTimeMillis();

        log.info("Using random seed: " + seed);

        random = new Random(seed);
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
     * Creates a storage instanc efor testing.
     */
    protected abstract SortedIndexMvStorage createIndexStorage(String name, TableView tableCfg);

    /**
     * Creates a Sorted Index using the given columns.
     */
    private SortedIndexMvStorage createIndexStorage(List<ColumnDefinition> indexSchema) {
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
    private SortedIndexMvStorage createIndexStorage(ColumnarIndexDefinition indexDefinition) {
        CompletableFuture<Void> createIndexFuture = tableCfg.change(cfg ->
                cfg.changeIndices(idxList ->
                        idxList.create(indexDefinition.name(), idx -> convert(indexDefinition, idx))));

        assertThat(createIndexFuture, willBe(nullValue(Void.class)));

        return createIndexStorage(indexDefinition.name(), tableCfg.value());
    }

    /**
     * Tests that columns of all types are correctly serialized and deserialized.
     */
    @Test
    void testRowSerialization() {
        SortedIndexMvStorage indexStorage = createIndexStorage(ALL_TYPES_COLUMN_DEFINITIONS);

        Object[] columns = indexStorage.indexDescriptor().indexColumns().stream()
                .map(ColumnDescriptor::type)
                .map(type -> SchemaTestUtils.generateRandomValue(random, type))
                .toArray();

        IndexRow row = indexStorage.indexRowSerializer().createIndexRow(columns, new TestRowId(0));

        Object[] actual = indexStorage.indexRowDeserializer().deserializeColumns(row);

        assertThat(actual, is(equalTo(columns)));
    }

    @Test
    public void testEmpty() throws Exception {
        SortedIndexMvStorage index = createIndexStorage(shuffledRandomDefinitions());

        assertThat(scan(index, null, null, 0), is(empty()));
    }

    /**
     * Tests the Put-Get-Remove case when an index is created using a single column.
     */
    @ParameterizedTest
    @VariableSource("ALL_TYPES_COLUMN_DEFINITIONS")
    void testCreateIndex(ColumnDefinition columnDefinition) throws Exception {
        testPutGetRemove(List.of(columnDefinition));
    }

    /**
     * Tests that it adding a row that already exists does not do anything.
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

        SortedIndexMvStorage index = createIndexStorage(indexDefinition);

        var columnValues = new Object[] { "foo", 1 };
        var rowId = new TestRowId(0);

        IndexRow row = index.indexRowSerializer().createIndexRow(columnValues, rowId);

        index.put(row);
        index.put(row);

        IndexRow actualRow = getSingle(index, () -> columnValues);

        assertThat(actualRow.indexBytes(), is(equalTo(row.indexBytes())));
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

        SortedIndexMvStorage index = createIndexStorage(indexDefinition);

        var columnValues1 = new Object[] { "foo", 1 };
        var columnValues2 = new Object[] { "bar", 3 };
        var rowId1 = new TestRowId(0);
        var rowId2 = new TestRowId(0);
        var rowId3 = new TestRowId(0);

        IndexRow row1 = index.indexRowSerializer().createIndexRow(columnValues1, rowId1);
        IndexRow row2 = index.indexRowSerializer().createIndexRow(columnValues1, rowId2);
        IndexRow row3 = index.indexRowSerializer().createIndexRow(columnValues2, rowId3);

        index.put(row1);
        index.put(row2);
        index.put(row3);

        List<Object[]> actualColumns = scan(index, null, null, 0);

        assertThat(actualColumns, contains(columnValues2, columnValues1, columnValues1));
    }

    /**
     * Tests the {@link SortedIndexMvStorage#remove} method.
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

        SortedIndexMvStorage index = createIndexStorage(indexDefinition);

        var columnValues1 = new Object[] { "foo", 1 };
        var columnValues2 = new Object[] { "bar", 3 };
        var rowId1 = new TestRowId(0);
        var rowId2 = new TestRowId(0);
        var rowId3 = new TestRowId(0);

        IndexRow row1 = index.indexRowSerializer().createIndexRow(columnValues1, rowId1);
        IndexRow row2 = index.indexRowSerializer().createIndexRow(columnValues1, rowId2);
        IndexRow row3 = index.indexRowSerializer().createIndexRow(columnValues2, rowId3);

        index.put(row1);
        index.put(row2);
        index.put(row3);

        List<Object[]> actualColumns = scan(index, null, null, 0);

        assertThat(actualColumns, contains(columnValues2, columnValues1, columnValues1));

        // Test that rows with the same indexed columns can be removed individually
        index.remove(row2);

        actualColumns = scan(index, null, null, 0);

        assertThat(actualColumns, contains(columnValues2, columnValues1));

        // Test that removing a non-existent row does nothing
        index.remove(row2);

        actualColumns = scan(index, null, null, 0);

        assertThat(actualColumns, contains(columnValues2, columnValues1));

        // Test that the first row can be actually removed
        index.remove(row1);

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
     * Tests the happy case of the {@link SortedIndexMvStorage#scan} method.
     */
    @RepeatedTest(5)
    void testScan() throws Exception {
        SortedIndexMvStorage indexStorage = createIndexStorage(shuffledDefinitions());

        List<TestIndexRow> entries = IntStream.range(0, 10)
                .mapToObj(i -> {
                    TestIndexRow entry = TestIndexRow.randomRow(indexStorage);

                    indexStorage.put(entry);

                    return entry;
                })
                .sorted()
                .collect(toList());

        int firstIndex = 3;
        int lastIndex = 8;

        List<ByteBuffer> expected = entries.stream()
                .skip(firstIndex)
                .limit(lastIndex - firstIndex + 1)
                .map(TestIndexRow::indexBytes)
                .collect(toList());

        IndexRowPrefix first = entries.get(firstIndex).prefix(3);
        IndexRowPrefix last = entries.get(lastIndex).prefix(5);

        try (Cursor<IndexRow> cursor = indexStorage.scan(first, last, GREATER_OR_EQUAL | LESS_OR_EQUAL)) {
            List<ByteBuffer> actual = cursor.stream()
                    .map(IndexRow::indexBytes)
                    .collect(toList());

            assertThat(actual, hasSize(lastIndex - firstIndex + 1));

            for (int i = firstIndex; i < actual.size(); ++i) {
                assertThat(actual.get(i), is(equalTo(expected.get(i))));
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

        SortedIndexMvStorage index1 = createIndexStorage(index1Definition);
        SortedIndexMvStorage index2 = createIndexStorage(index2Definition);

        Object[] val9010 = { "10", 90 };
        Object[] val8010 = { "10", 80 };
        Object[] val9020 = { "20", 90 };
        Object[] val8020 = { "20", 80 };

        for (SortedIndexMvStorage index : Arrays.asList(index1, index2)) {
            IndexRowSerializer serializer = index.indexRowSerializer();

            index.put(serializer.createIndexRow(val9010, new TestRowId(0)));
            index.put(serializer.createIndexRow(val8010, new TestRowId(0)));
            index.put(serializer.createIndexRow(val9020, new TestRowId(0)));
            index.put(serializer.createIndexRow(val8020, new TestRowId(0)));
        }

        // Test without bounds.
        assertThat(
                scan(index1, null, null, FORWARD),
                contains(val8010, val9010, val8020, val9020)
        );

        assertThat(
                scan(index1, null, null, BACKWARDS),
                contains(val9020, val8020, val9010, val8010)
        );

        assertThat(
                scan(index2, null, null, FORWARD),
                contains(val9010, val8010, val9020, val8020)
        );

        assertThat(
                scan(index2, null, null, BACKWARDS),
                contains(val8020, val9020, val8010, val9010)
        );

        // Lower bound exclusive.)
        assertThat(
                scan(index1, prefix(index1, "10"), null, GREATER | FORWARD),
                contains(val8020, val9020)
        );

        assertThat(
                scan(index1, prefix(index1, "10"), null, GREATER | BACKWARDS),
                contains(val9020, val8020)
        );

        assertThat(
                scan(index2, prefix(index2, "10"), null, GREATER | FORWARD),
                contains(val9020, val8020)
        );

        assertThat(
                scan(index2, prefix(index2, "10"), null, GREATER | BACKWARDS),
                contains(val8020, val9020)
        );

        // Lower bound inclusive.
        assertThat(
                scan(index1, prefix(index1, "10"), null, GREATER_OR_EQUAL | FORWARD),
                contains(val8010, val9010, val8020, val9020)
        );

        assertThat(
                scan(index1, prefix(index1, "10"), null, GREATER_OR_EQUAL | BACKWARDS),
                contains(val9020, val8020, val9010, val8010)
        );

        assertThat(
                scan(index2, prefix(index2, "10"), null, GREATER_OR_EQUAL | FORWARD),
                contains(val9010, val8010, val9020, val8020)
        );

        assertThat(
                scan(index2, prefix(index2, "10"), null, GREATER_OR_EQUAL | BACKWARDS),
                contains(val8020, val9020, val8010, val9010)
        );

        // Upper bound exclusive.
        assertThat(
                scan(index1, null, prefix(index1, "20"), LESS | FORWARD),
                contains(val8010, val9010)
        );

        assertThat(
                scan(index1, null, prefix(index1, "20"), LESS | BACKWARDS),
                contains(val9010, val8010)
        );

        assertThat(
                scan(index2, null, prefix(index2, "20"), LESS | FORWARD),
                contains(val9010, val8010)
        );

        assertThat(
                scan(index2, null, prefix(index2, "20"), LESS | BACKWARDS),
                contains(val8010, val9010)
        );

        // Upper bound inclusive.
        assertThat(
                scan(index1, null, prefix(index1, "20"), LESS_OR_EQUAL | FORWARD),
                contains(val8010, val9010, val8020, val9020)
        );

        assertThat(
                scan(index1, null, prefix(index1, "20"), LESS_OR_EQUAL | BACKWARDS),
                contains(val9020, val8020, val9010, val8010)
        );

        assertThat(
                scan(index2, null, prefix(index2, "20"), LESS_OR_EQUAL | FORWARD),
                contains(val9010, val8010, val9020, val8020)
        );

        assertThat(
                scan(index2, null, prefix(index2, "20"), LESS_OR_EQUAL | BACKWARDS),
                contains(val8020, val9020, val8010, val9010)
        );
    }

    /**
     * Tests that an empty range is returned if {@link SortedIndexStorage#range} method is called using overlapping keys.
     */
    @Test
    void testEmptyRange() throws Exception {
        List<ColumnDefinition> indexSchema = shuffledRandomDefinitions();

        SortedIndexMvStorage indexStorage = createIndexStorage(indexSchema);

        TestIndexRow entry1 = TestIndexRow.randomRow(indexStorage);
        TestIndexRow entry2 = TestIndexRow.randomRow(indexStorage);

        if (entry2.compareTo(entry1) < 0) {
            TestIndexRow t = entry2;
            entry2 = entry1;
            entry1 = t;
        }

        indexStorage.put(entry1);
        indexStorage.put(entry2);

        try (Cursor<IndexRow> cursor = indexStorage.scan(entry2::columns, entry1::columns, 0)) {
            assertThat(cursor.stream().collect(toList()), is(empty()));
        }
    }

    @ParameterizedTest
    @VariableSource("ALL_TYPES_COLUMN_DEFINITIONS")
    void testNullValues(ColumnDefinition columnDefinition) throws Exception {
        SortedIndexMvStorage storage = createIndexStorage(List.of(columnDefinition));

        TestIndexRow entry1 = TestIndexRow.randomRow(storage);

        Object[] nullArray = new Object[storage.indexDescriptor().indexColumns().size()];

        IndexRow nullRow = storage.indexRowSerializer().createIndexRow(nullArray, new TestRowId(0));

        TestIndexRow entry2 = new TestIndexRow(storage, nullRow, nullArray);

        storage.put(entry1);
        storage.put(entry2);

        if (entry1.compareTo(entry2) > 0) {
            TestIndexRow t = entry2;
            entry2 = entry1;
            entry1 = t;
        }

        try (Cursor<IndexRow> cursor = storage.scan(entry1::columns, entry2::columns, GREATER_OR_EQUAL | LESS_OR_EQUAL)) {
            assertThat(
                    cursor.stream().map(IndexRow::indexBytes).collect(toList()),
                    contains(entry1.indexBytes(), entry2.indexBytes())
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
        SortedIndexMvStorage indexStorage = createIndexStorage(indexSchema);

        TestIndexRow entry1 = TestIndexRow.randomRow(indexStorage);
        TestIndexRow entry2;

        // using a cycle here to protect against equal keys being generated
        do {
            entry2 = TestIndexRow.randomRow(indexStorage);
        } while (entry1.equals(entry2));

        indexStorage.put(entry1);
        indexStorage.put(entry2);

        assertThat(
                getSingle(indexStorage, entry1::columns).indexBytes(),
                is(equalTo(entry1.indexBytes()))
        );

        assertThat(
                getSingle(indexStorage, entry2::columns).indexBytes(),
                is(equalTo(entry2.indexBytes()))
        );

        indexStorage.remove(entry1);

        assertThat(getSingle(indexStorage, entry1::columns), is(nullValue()));
    }

    /**
     * Extracts a single value by a given key or {@code null} if it does not exist.
     */
    @Nullable
    private static IndexRow getSingle(SortedIndexMvStorage indexStorage, IndexRowPrefix fullPrefix) throws Exception {
        try (Cursor<IndexRow> cursor = indexStorage.scan(fullPrefix, fullPrefix, GREATER_OR_EQUAL | LESS_OR_EQUAL)) {
            List<IndexRow> values = cursor.stream().collect(toList());

            assertThat(values, anyOf(empty(), hasSize(1)));

            return values.isEmpty() ? null : values.get(0);
        }
    }

    private static IndexRowPrefix prefix(SortedIndexMvStorage index, Object... vals) {
        return index.indexRowSerializer().createIndexRowPrefix(vals);
    }

    private static List<Object[]> scan(
            SortedIndexMvStorage index,
            @Nullable IndexRowPrefix lowerBound,
            @Nullable IndexRowPrefix upperBound,
            @MagicConstant(flagsFromClass = SortedIndexMvStorage.class) int flags
    ) throws Exception {
        IndexRowDeserializer deserializer = index.indexRowDeserializer();

        try (Cursor<IndexRow> cursor = index.scan(lowerBound, upperBound, flags)) {
            return cursor.stream()
                    .map(deserializer::deserializeColumns)
                    .collect(toUnmodifiableList());
        }
    }
}
