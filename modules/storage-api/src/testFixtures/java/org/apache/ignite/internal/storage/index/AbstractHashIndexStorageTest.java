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
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders.column;
import static org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders.tableBuilder;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.impl.BinaryTupleRowSerializer;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.index.HashIndexDefinition;
import org.junit.jupiter.api.Test;

/**
 * Base class for Hash Index storage tests.
 */
public abstract class AbstractHashIndexStorageTest {
    private static final int TEST_PARTITION = 0;

    private static final String INT_COLUMN_NAME = "intVal";

    private static final String STR_COLUMN_NAME = "strVal";

    private MvTableStorage tableStorage;

    private MvPartitionStorage partitionStorage;

    private HashIndexStorage indexStorage;

    private BinaryTupleRowSerializer serializer;

    protected void initialize(MvTableStorage tableStorage) {
        this.tableStorage = tableStorage;

        createTestTable(tableStorage.configuration());

        this.partitionStorage = tableStorage.getOrCreateMvPartition(TEST_PARTITION);
        this.indexStorage = createIndex(tableStorage);
        this.serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());
    }

    /**
     * Configures a test table with some indexed columns.
     */
    private static void createTestTable(TableConfiguration tableCfg) {
        ColumnDefinition pkColumn = column("pk", ColumnType.INT32).asNullable(false).build();

        ColumnDefinition[] allColumns = {
                pkColumn,
                column(INT_COLUMN_NAME, ColumnType.INT32).asNullable(true).build(),
                column(STR_COLUMN_NAME, ColumnType.string()).asNullable(true).build()
        };

        TableDefinition tableDefinition = tableBuilder("test", "foo")
                .columns(allColumns)
                .withPrimaryKey(pkColumn.name())
                .build();

        CompletableFuture<Void> createTableFuture = tableCfg.change(cfg -> convert(tableDefinition, cfg));

        assertThat(createTableFuture, willCompleteSuccessfully());
    }

    /**
     * Configures and creates a storage instance for testing.
     */
    private static HashIndexStorage createIndex(MvTableStorage tableStorage) {
        HashIndexDefinition indexDefinition = SchemaBuilders.hashIndex("hashIndex")
                .withColumns(INT_COLUMN_NAME, STR_COLUMN_NAME)
                .build();

        CompletableFuture<Void> createIndexFuture = tableStorage.configuration()
                .change(cfg -> cfg.changeIndices(idxList ->
                        idxList.create(indexDefinition.name(), idx -> convert(indexDefinition, idx))));

        assertThat(createIndexFuture, willCompleteSuccessfully());

        TableIndexView indexConfig = tableStorage.configuration().indices().get(indexDefinition.name()).value();

        return tableStorage.getOrCreateHashIndex(TEST_PARTITION, indexConfig.id());
    }

    /**
     * Tests the {@link HashIndexStorage#get} method.
     */
    @Test
    public void testGet() {
        // First two rows have the same index key, but different row IDs
        IndexRow row1 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));
        IndexRow row2 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));
        IndexRow row3 = serializer.serializeRow(new Object[]{ 2, "bar" }, new RowId(TEST_PARTITION));

        assertThat(getAll(row1), is(empty()));
        assertThat(getAll(row2), is(empty()));
        assertThat(getAll(row3), is(empty()));

        put(row1);
        put(row2);
        put(row3);

        assertThat(getAll(row1), containsInAnyOrder(row1.rowId(), row2.rowId()));
        assertThat(getAll(row2), containsInAnyOrder(row1.rowId(), row2.rowId()));
        assertThat(getAll(row3), contains(row3.rowId()));
    }

    /**
     * Tests that {@link HashIndexStorage#put} does not create row ID duplicates.
     */
    @Test
    public void testPutIdempotence() {
        IndexRow row = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));

        put(row);
        put(row);

        assertThat(getAll(row), contains(row.rowId()));
    }

    /**
     * Tests the {@link HashIndexStorage#remove} method.
     */
    @Test
    public void testRemove() {
        IndexRow row1 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));
        IndexRow row2 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));
        IndexRow row3 = serializer.serializeRow(new Object[]{ 2, "bar" }, new RowId(TEST_PARTITION));

        put(row1);
        put(row2);
        put(row3);

        assertThat(getAll(row1), containsInAnyOrder(row1.rowId(), row2.rowId()));
        assertThat(getAll(row2), containsInAnyOrder(row1.rowId(), row2.rowId()));
        assertThat(getAll(row3), contains(row3.rowId()));

        remove(row1);

        assertThat(getAll(row1), contains(row2.rowId()));
        assertThat(getAll(row2), contains(row2.rowId()));
        assertThat(getAll(row3), contains(row3.rowId()));

        remove(row2);

        assertThat(getAll(row1), is(empty()));
        assertThat(getAll(row2), is(empty()));
        assertThat(getAll(row3), contains(row3.rowId()));

        remove(row3);

        assertThat(getAll(row1), is(empty()));
        assertThat(getAll(row2), is(empty()));
        assertThat(getAll(row3), is(empty()));
    }

    /**
     * Tests that {@link HashIndexStorage#remove} works normally when removing a non-existent row.
     */
    @Test
    public void testRemoveIdempotence() {
        IndexRow row = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));

        assertDoesNotThrow(() -> remove(row));

        put(row);

        remove(row);

        assertThat(getAll(row), is(empty()));

        assertDoesNotThrow(() -> remove(row));
    }

    @Test
    public void testDestroy() throws Exception {
        IndexRow row1 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));
        IndexRow row2 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(TEST_PARTITION));
        IndexRow row3 = serializer.serializeRow(new Object[]{ 2, "bar" }, new RowId(TEST_PARTITION));

        put(row1);
        put(row2);
        put(row3);

        CompletableFuture<Void> destroyFuture = tableStorage.destroyIndex(indexStorage.indexDescriptor().id());

        waitForDurableCompletion(destroyFuture);

        //TODO IGNITE-17626 Index must be invalid, we should assert that getIndex returns null and that in won't surface upon restart.
        // "destroy" is not "clear", you know. Maybe "getAndCreateIndex" will do it for the test, idk
        assertThat(getAll(row1), is(empty()));
        assertThat(getAll(row2), is(empty()));
        assertThat(getAll(row3), is(empty()));
    }

    private void waitForDurableCompletion(CompletableFuture<?> future) {
        while (true) {
            if (future.isDone()) {
                return;
            }

            partitionStorage.flush().join();
        }
    }

    private Collection<RowId> getAll(IndexRow row) {
        try (Cursor<RowId> cursor = indexStorage.get(row.indexColumns())) {
            return cursor.stream().collect(toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void put(IndexRow row) {
        partitionStorage.runConsistently(() -> {
            indexStorage.put(row);

            return null;
        });
    }

    private void remove(IndexRow row) {
        partitionStorage.runConsistently(() -> {
            indexStorage.remove(row);

            return null;
        });
    }
}
