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

package org.apache.ignite.internal.storage.rocksdb.index;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.configuration.ConfigurationTestUtils.fixConfiguration;
import static org.apache.ignite.internal.schema.SchemaTestUtils.generateRandomValue;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.schema.SchemaBuilders.column;
import static org.apache.ignite.schema.SchemaBuilders.tableBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.configuration.schemas.store.DataRegionConfiguration;
import org.apache.ignite.configuration.schemas.store.RocksDbDataRegionChange;
import org.apache.ignite.configuration.schemas.store.RocksDbDataRegionConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.idx.SortedIndexColumnDescriptor;
import org.apache.ignite.internal.idx.SortedIndexDescriptor;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.configuration.SchemaDescriptorConverter;
import org.apache.ignite.internal.storage.engine.DataRegion;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.testframework.VariableSource;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;

/**
 * Test class for the {@link RocksDbSortedIndexStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class RocksDbSortedIndexStorageTest {
    private static final IgniteLogger log = IgniteLogger.forClass(RocksDbSortedIndexStorageTest.class);

    /**
     * Definitions of all supported column types.
     */
    private static final List<ColumnDefinition> ALL_TYPES_COLUMN_DEFINITIONS = allTypesColumnDefinitions();

    /**
     * List of resources that need to be closed at the end of each test.
     */
    private final List<AutoCloseable> resources = new ArrayList<>();

    private Random random;
    @InjectConfiguration(polymorphicExtensions = {
            HashIndexConfigurationSchema.class,
            SortedIndexConfigurationSchema.class
    })
    private TableConfiguration tableCfg;
    /**
     * Table Storage for creating indices.
     */
    private TableStorage tableStorage;

    /**
     * Extracts all data from a given cursor and closes it.
     */
    private static <T> List<T> cursorToList(Cursor<T> cursor) throws Exception {
        try (cursor) {
            var list = new ArrayList<T>();

            cursor.forEachRemaining(list::add);

            return list;
        }
    }

    /**
     * Extracts a single value by a given key or {@code null} if it does not exist.
     */
    @Nullable
    private static IndexRow getSingle(SortedIndexStorage indexStorage, IndexRowWrapper entry) throws Exception {
        IndexRowPrefix fullPrefix = entry.prefix(((SortedIndexStorageDescriptor) indexStorage.indexDescriptor()).indexColumns().size());

        List<IndexRow> values = cursorToList(indexStorage.range(fullPrefix, fullPrefix, r -> true));

        assertThat(values, anyOf(empty(), hasSize(1)));

        return values.isEmpty() ? null : values.get(0);
    }

    @BeforeEach
    void setUp(
            @WorkDirectory Path workDir,
            @InjectConfiguration(polymorphicExtensions = RocksDbDataRegionConfigurationSchema.class) DataRegionConfiguration dataRegionCfg
    ) {
        long seed = System.currentTimeMillis();

        log.info("Using random seed: " + seed);

        random = new Random(seed);

        createTestConfiguration(dataRegionCfg);

        dataRegionCfg = fixConfiguration(dataRegionCfg);

        var engine = new RocksDbStorageEngine();

        engine.start();

        resources.add(engine::stop);

        DataRegion dataRegion = engine.createDataRegion(dataRegionCfg);

        dataRegion.start();

        resources.add(() -> {
            dataRegion.beforeNodeStop();
            dataRegion.stop();
        });

        tableStorage = engine.createTable(workDir, tableCfg, dataRegion);

        tableStorage.start();

        resources.add(tableStorage::stop);
    }

    /**
     * Configures a test table with columns of all supported types.
     */
    private void createTestConfiguration(DataRegionConfiguration dataRegionCfg) {
        CompletableFuture<Void> dataRegionChangeFuture = dataRegionCfg
                .change(cfg -> cfg.convert(RocksDbDataRegionChange.class).changeSize(16 * 1024).changeWriteBufferSize(16 * 1024));

        assertThat(dataRegionChangeFuture, willBe(nullValue(Void.class)));

        TableDefinition tableDefinition = tableBuilder("test", "foo")
                .columns(ALL_TYPES_COLUMN_DEFINITIONS.toArray(new ColumnDefinition[0]))
                .withPrimaryKey(ALL_TYPES_COLUMN_DEFINITIONS.get(0).name())
                .build();

        CompletableFuture<Void> createTableFuture = tableCfg.change(cfg -> convert(tableDefinition, cfg));

        assertThat(createTableFuture, willBe(nullValue(Void.class)));
    }

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
                ColumnType.blobOf(),
                ColumnType.numberOf(),
                ColumnType.decimalOf(),
                ColumnType.time(),
                ColumnType.datetime(),
                ColumnType.timestamp()
        );

        return allColumnTypes
                .map(type -> column(type.typeSpec().name(), type).asNullable(false).build())
                .collect(toUnmodifiableList());
    }

    @AfterEach
    void tearDown() throws Exception {
        Collections.reverse(resources);

        IgniteUtils.closeAll(resources);
    }

    /**
     * Tests that columns of all types are correctly serialized and deserialized.
     */
    @Test
    void testRowSerialization() {
        SortedIndexStorage indexStorage = createIndex(ALL_TYPES_COLUMN_DEFINITIONS);

        Tuple tuple = Tuple.create();
        ((SortedIndexStorageDescriptor) indexStorage.indexDescriptor()).indexColumns().stream()
                .sequential()
                .map(SortedIndexColumnDescriptor::column)
                .forEach(column -> tuple.set(column.name(), generateRandomValue(random, column.type())));

        BinaryIndexRowSerializer ser = new BinaryIndexRowSerializer((SortedIndexStorageDescriptor) indexStorage.indexDescriptor());

        IndexBinaryRow binRow = ser.serialize(new TestIndexRow(tuple, new ByteBufferRow(new byte[]{(byte) 0}), 0));

        IndexRow idxRow = ser.deserialize(binRow);

        for (int i = 0; i < ((SortedIndexStorageDescriptor) indexStorage.indexDescriptor()).indexColumns().size(); ++i) {
            assertThat(tuple.value(i), is(equalTo(idxRow.value(i))));
        }
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
     * Tests the Put-Get-Remove case when an index is created using all possible column in random order.
     */
    @RepeatedTest(5)
    void testCreateMultiColumnIndex() throws Exception {
        testPutGetRemove(shuffledDefinitions());
    }

    /**
     * Tests the happy case of the {@link SortedIndexStorage#range} method.
     */
    @RepeatedTest(5)
    void testRange() throws Exception {
        List<ColumnDefinition> indexSchema = shuffledDefinitions();

        SortedIndexStorage indexStorage = createIndex(indexSchema);

        List<IndexRowWrapper> entries = IntStream.range(0, 10)
                .mapToObj(i -> {
                    IndexRowWrapper entry = IndexRowWrapper.randomRow(indexStorage);

                    indexStorage.put(entry.row());

                    return entry;
                })
                .sorted()
                .collect(Collectors.toList());

        int firstIndex = 3;
        int lastIndex = 8;

        List<byte[]> expected = entries.stream()
                .skip(firstIndex)
                .limit(lastIndex - firstIndex + 1)
                .map(e -> e.row().primaryKey().bytes())
                .collect(Collectors.toList());

        IndexRowPrefix first = entries.get(firstIndex).prefix(3);
        IndexRowPrefix last = entries.get(lastIndex).prefix(5);

        List<byte[]> actual = cursorToList(indexStorage.range(first, last, r -> true))
                .stream()
                .map(IndexRow::primaryKey)
                .map(BinaryRow::bytes)
                .collect(Collectors.toList());

        assertThat(actual, hasSize(lastIndex - firstIndex + 1));

        for (int i = firstIndex; i < actual.size(); ++i) {
            assertThat(actual.get(i), is(equalTo(expected.get(i))));
        }
    }

    /**
     * Tests that an empty range is returned if {@link SortedIndexStorage#range} method is called using overlapping keys.
     */
    @Test
    void testEmptyRange() throws Exception {
        List<ColumnDefinition> indexSchema = shuffledRandomDefinitions();

        SortedIndexStorage indexStorage = createIndex(indexSchema);

        IndexRowWrapper entry1 = IndexRowWrapper.randomRow(indexStorage);
        IndexRowWrapper entry2 = IndexRowWrapper.randomRow(indexStorage);

        if (entry2.compareTo(entry1) < 0) {
            IndexRowWrapper t = entry2;
            entry2 = entry1;
            entry1 = t;
        }

        indexStorage.put(entry1.row());
        indexStorage.put(entry2.row());

        int colCount = ((SortedIndexStorageDescriptor) indexStorage.indexDescriptor()).indexColumns().size();

        List<IndexRow> actual = cursorToList(indexStorage.range(entry2.prefix(colCount), entry1.prefix(colCount), r -> true));

        assertThat(actual, is(empty()));
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
                .collect(Collectors.toList());

        if (shuffledDefinitions.isEmpty()) {
            shuffledDefinitions = new ArrayList<>(ALL_TYPES_COLUMN_DEFINITIONS);
        }

        Collections.shuffle(shuffledDefinitions, random);

        if (log.isInfoEnabled()) {
            List<String> columnNames = shuffledDefinitions.stream().map(ColumnDefinition::name).collect(Collectors.toList());

            log.info("Creating index with the following column order: " + columnNames);
        }

        return shuffledDefinitions;
    }

    /**
     * Tests the Get-Put-Remove scenario: inserts some keys into the storage and checks that they have been successfully persisted and can
     * be removed.
     */
    private void testPutGetRemove(List<ColumnDefinition> indexSchema) throws Exception {
        SortedIndexStorage indexStorage = createIndex(indexSchema);

        IndexRowWrapper entry1 = IndexRowWrapper.randomRow(indexStorage);
        IndexRowWrapper entry2;

        // using a cycle here to protect against equal keys being generated
        do {
            entry2 = IndexRowWrapper.randomRow(indexStorage);
        } while (entry1.equals(entry2));

        indexStorage.put(entry1.row());
        indexStorage.put(entry2.row());

        assertThat(
                getSingle(indexStorage, entry1).primaryKey().bytes(),
                is(equalTo(entry1.row().primaryKey().bytes()))
        );

        assertThat(
                getSingle(indexStorage, entry2).primaryKey().bytes(),
                is(equalTo(entry2.row().primaryKey().bytes()))
        );

        indexStorage.remove(entry1.row());

        assertThat(getSingle(indexStorage, entry1), is(nullValue()));
    }

    /**
     * Creates a Sorted Index using the given columns.
     */
    private SortedIndexStorage createIndex(List<ColumnDefinition> indexSchema) {
        List<SortedIndexColumnDescriptor> cols = new ArrayList<>();

        for (int i = 0; i < indexSchema.size(); ++i) {
            ColumnDefinition colDef = indexSchema.get(i);

            Column col = new Column(colDef.name(), SchemaDescriptorConverter.convert(colDef.type()), true);
            cols.add(new SortedIndexColumnDescriptor(col, random.nextBoolean()));
        }

        return tableStorage.createSortedIndex(
                new SortedIndexDescriptor("foo", cols, new Column[] {cols.get(0).column()})
        );
    }
}
