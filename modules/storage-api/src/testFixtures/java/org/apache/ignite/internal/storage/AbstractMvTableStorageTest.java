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

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.EntryCountBudgetConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.schemas.table.UnlimitedBudgetConfigurationSchema;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Abstract class that contains tests for {@link MvTableStorage} implementations.
 */
// TODO: Use this to test B+tree-based storage, see https://issues.apache.org/jira/browse/IGNITE-17320
public abstract class AbstractMvTableStorageTest extends BaseMvStoragesTest {
    private static final String SORTED_INDEX_NAME = "SORTED_IDX";

    private static final String HASH_INDEX_NAME = "HASH_IDX";

    protected static final int PARTITION_ID = 0;

    /** Partition id for 0 storage. */
    protected static final int PARTITION_ID_0 = 42;

    /** Partition id for 1 storage. */
    protected static final int PARTITION_ID_1 = 1 << 8;

    protected MvTableStorage tableStorage;

    protected TableIndexView sortedIdx;

    protected TableIndexView hashIdx;

    protected TableConfiguration tableConfig;

    protected abstract MvTableStorage tableStorage(TableIndexView sortedIdx, TableIndexView hashIdx, TablesConfiguration tablesCfg);

    protected abstract void setUp();

    /** Configuration registry with one table for each test. */
    private ConfigurationRegistry confRegistry;

    @BeforeEach
    void setUpBase() {
        startTestRegistry();

        TablesConfiguration tablesCfg = confRegistry.getConfiguration(TablesConfiguration.KEY);

        createTestIndexes();

        sortedIdx = tablesCfg.indexes().get(SORTED_INDEX_NAME).value();
        hashIdx = tablesCfg.indexes().get(HASH_INDEX_NAME).value();

        setUp();

        createTestTable();

        tableStorage = tableStorage(sortedIdx, hashIdx, tablesCfg);

        tableStorage.start();
    }

    @AfterEach
    void tearDownBase() {
        tableStorage.stop();
    }

    /**
     * Tests that {@link MvTableStorage#getMvPartition(int)} correctly returns an existing partition.
     */
    @Test
    void testCreatePartition() {
        MvPartitionStorage absentStorage = tableStorage.getMvPartition(0);

        assertThat(absentStorage, is(nullValue()));

        MvPartitionStorage partitionStorage = tableStorage.getOrCreateMvPartition(0);

        assertThat(partitionStorage, is(notNullValue()));

        assertThat(partitionStorage, is(sameInstance(tableStorage.getMvPartition(0))));
    }

    /**
     * Tests that partition data does not overlap.
     */
    @Test
    void testPartitionIndependence() throws Exception {
        MvPartitionStorage partitionStorage0 = tableStorage.getOrCreateMvPartition(PARTITION_ID_0);
        // Using a shifted ID value to test a multibyte scenario.
        MvPartitionStorage partitionStorage1 = tableStorage.getOrCreateMvPartition(PARTITION_ID_1);

        var testData0 = binaryRow(new TestKey(1, "1"), new TestValue(10, "10"));

        UUID txId = UUID.randomUUID();

        RowId rowId0 = new RowId(PARTITION_ID_0);

        partitionStorage0.runConsistently(() -> partitionStorage0.addWrite(rowId0, testData0, txId));

        assertThat(unwrap(partitionStorage0.read(rowId0, txId)), is(equalTo(unwrap(testData0))));
        assertThat(partitionStorage1.read(rowId0, txId), is(nullValue()));

        var testData1 = binaryRow(new TestKey(2, "2"), new TestValue(20, "20"));

        RowId rowId1 = new RowId(PARTITION_ID_1);

        partitionStorage1.runConsistently(() -> partitionStorage1.addWrite(rowId1, testData1, txId));

        assertThat(partitionStorage0.read(rowId1, txId), is(nullValue()));
        assertThat(unwrap(partitionStorage1.read(rowId1, txId)), is(equalTo(unwrap(testData1))));

        assertThat(toList(partitionStorage0.scan(row -> true, txId)), contains(unwrap(testData0)));
        assertThat(toList(partitionStorage1.scan(row -> true, txId)), contains(unwrap(testData1)));
    }

    /**
     * Test creating a Sorted Index.
     */
    @Test
    public void testCreateSortedIndex() {
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id()));

        // Index should only be available after the associated partition has been created.
        tableStorage.getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id()), is(notNullValue()));
    }

    /**
     * Test creating a Hash Index.
     */
    @Test
    public void testCreateHashIndex() {
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id()));

        // Index should only be available after the associated partition has been created.
        tableStorage.getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id()), is(notNullValue()));
    }

    /**
     * Tests destroying an index.
     */
    @Test
    public void testDestroyIndex() {
        tableStorage.getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id()), is(notNullValue()));
        assertThat(tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id()), is(notNullValue()));

        assertThat(tableStorage.destroyIndex(sortedIdx.id()), willCompleteSuccessfully());
        assertThat(tableStorage.destroyIndex(hashIdx.id()), willCompleteSuccessfully());
    }

    @Test
    public void testHashIndexIndependence() {
        MvPartitionStorage partitionStorage1 = tableStorage.getOrCreateMvPartition(PARTITION_ID);

        assertThat(tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id()), is(notNullValue()));
        assertThrows(StorageException.class, () -> tableStorage.getOrCreateHashIndex(PARTITION_ID + 1, hashIdx.id()));

        MvPartitionStorage partitionStorage2 = tableStorage.getOrCreateMvPartition(PARTITION_ID + 1);

        HashIndexStorage storage1 = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id());
        HashIndexStorage storage2 = tableStorage.getOrCreateHashIndex(PARTITION_ID + 1, hashIdx.id());

        assertThat(storage1, is(notNullValue()));
        assertThat(storage2, is(notNullValue()));

        var rowId1 = new RowId(PARTITION_ID);
        var rowId2 = new RowId(PARTITION_ID + 1);

        BinaryTupleSchema schema = BinaryTupleSchema.create(new Element[] {
                new Element(NativeTypes.INT32, false),
                new Element(NativeTypes.INT32, false)
        });

        ByteBuffer buffer = BinaryTupleBuilder.create(schema.elementCount(), schema.hasNullableElements())
                .appendInt(1)
                .appendInt(2)
                .build();

        BinaryTuple tuple = new BinaryTuple(schema, buffer);

        partitionStorage1.runConsistently(() -> {
            storage1.put(new IndexRowImpl(tuple, rowId1));

            return null;
        });

        partitionStorage2.runConsistently(() -> {
            storage2.put(new IndexRowImpl(tuple, rowId2));

            return null;
        });

        assertThat(getAll(storage1.get(tuple)), contains(rowId1));
        assertThat(getAll(storage2.get(tuple)), contains(rowId2));

        assertThat(tableStorage.destroyIndex(sortedIdx.id()), willCompleteSuccessfully());
    }

    /**
     * Tests that exceptions are thrown if indices are not configured correctly.
     */
    @Test
    public void testMisconfiguredIndices() {
        Exception e = assertThrows(
                StorageException.class,
                () -> tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx.id())
        );

        assertThat(e.getMessage(), is("Partition ID " + PARTITION_ID + " does not exist"));

        e = assertThrows(
                StorageException.class,
                () -> tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx.id())
        );

        assertThat(e.getMessage(), is("Partition ID " + PARTITION_ID + " does not exist"));

        tableStorage.getOrCreateMvPartition(PARTITION_ID);

        UUID invalidUuid = UUID.randomUUID();

        e = assertThrows(
                StorageException.class,
                () -> tableStorage.getOrCreateHashIndex(PARTITION_ID, invalidUuid)
        );

        assertThat(e.getMessage(), is(String.format("Index configuration for \"%s\" could not be found", invalidUuid)));

        e = assertThrows(
                StorageException.class,
                () -> tableStorage.getOrCreateHashIndex(PARTITION_ID, sortedIdx.id())
        );

        assertThat(
                e.getMessage(),
                is(String.format("Index \"%s\" is not configured as a Hash Index. Actual type: SORTED", sortedIdx.id()))
        );

        e = assertThrows(
                StorageException.class,
                () -> tableStorage.getOrCreateSortedIndex(PARTITION_ID, hashIdx.id())
        );

        assertThat(
                e.getMessage(),
                is(String.format("Index \"%s\" is not configured as a Sorted Index. Actual type: HASH", hashIdx.id()))
        );
    }

    private void startTestRegistry() {
        confRegistry = new ConfigurationRegistry(
                List.of(TablesConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of(),
                List.of(
                        HashIndexConfigurationSchema.class,
                        SortedIndexConfigurationSchema.class,
                        UnknownDataStorageConfigurationSchema.class,
                        ConstantValueDefaultConfigurationSchema.class,
                        FunctionCallDefaultConfigurationSchema.class,
                        NullValueDefaultConfigurationSchema.class,
                        UnlimitedBudgetConfigurationSchema.class,
                        EntryCountBudgetConfigurationSchema.class
                )
        );

        confRegistry.start();
    }

    private void createTestIndexes() {
        CompletableFuture<Void> indexCreateFut = confRegistry.getConfiguration(TablesConfiguration.KEY).indexes().change(ch -> {
            List.of(SchemaBuilders.sortedIndex(SORTED_INDEX_NAME)
                            .addIndexColumn("COLUMN0").done()
                            .build(),
                    SchemaBuilders.hashIndex(HASH_INDEX_NAME)
                            .withColumns("COLUMN0")
                            .build()
            ).forEach(idxDef -> ch.create(idxDef.name(), c ->
                    SchemaConfigurationConverter.addIndex(idxDef, UUID.randomUUID(), c)));
        });

        assertThat(indexCreateFut, willCompleteSuccessfully());
    }

    private void createTestTable() {
        TableDefinition tableDefinition = SchemaBuilders.tableBuilder("PUBLIC", "TEST")
                .columns(
                        SchemaBuilders.column("ID", ColumnType.INT32).build(),
                        SchemaBuilders.column("COLUMN0", ColumnType.INT32).build()
                )
                .withPrimaryKey("ID")
                .build();

        tableConfig.change(tblChg -> SchemaConfigurationConverter.convert(tableDefinition, tblChg));

        CompletableFuture<Void> createTableFuture = confRegistry.getConfiguration(TablesConfiguration.KEY).tables()
                .change(chg -> chg.create(tableDefinition.canonicalName(),
                        tblChg -> SchemaConfigurationConverter.convert(tableDefinition, tblChg)));

        assertThat(createTableFuture, willCompleteSuccessfully());
    }

    private static <T> List<T> getAll(Cursor<T> cursor) {
        try (cursor) {
            return cursor.stream().collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
