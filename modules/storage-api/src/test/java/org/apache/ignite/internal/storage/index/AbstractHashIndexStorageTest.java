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

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.schema.SchemaBuilders.column;
import static org.apache.ignite.schema.SchemaBuilders.tableBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.chm.TestConcurrentHashMapStorageEngine;
import org.apache.ignite.internal.storage.chm.schema.TestConcurrentHashMapDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.index.impl.BinaryTupleRowSerializer;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.index.HashIndexDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for Hash Index storage tests.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class AbstractHashIndexStorageTest {
    private static final String INT_COLUMN_NAME = "intVal";

    private static final String STR_COLUMN_NAME = "strVal";

    private TableConfiguration tableCfg;

    private HashIndexStorage indexStorage;

    private BinaryTupleRowSerializer serializer;

    @BeforeEach
    void setUp(@InjectConfiguration(
            polymorphicExtensions = {
                    HashIndexConfigurationSchema.class,
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
        this.indexStorage = createIndexStorage();
        this.serializer = new BinaryTupleRowSerializer(indexStorage.indexDescriptor());
    }

    /**
     * Configures a test table with columns of all supported types.
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
     * Creates a storage instance for testing.
     */
    protected abstract HashIndexStorage createIndexStorage(String name, TableView tableCfg);

    /**
     * Creates a Sorted Index using the given columns.
     */
    private HashIndexStorage createIndexStorage() {
        HashIndexDefinition indexDefinition = SchemaBuilders.hashIndex("hashIndex")
                .withColumns(INT_COLUMN_NAME, STR_COLUMN_NAME)
                .build();

        CompletableFuture<Void> createIndexFuture = tableCfg.change(cfg ->
                cfg.changeIndices(idxList ->
                        idxList.create(indexDefinition.name(), idx -> convert(indexDefinition, idx))));

        assertThat(createIndexFuture, willBe(nullValue(Void.class)));

        return createIndexStorage(indexDefinition.name(), tableCfg.value());
    }

    /**
     * Tests the {@link HashIndexStorage#get} method.
     */
    @Test
    void testGet() {
        // First two rows have the same index key, but different row IDs
        IndexRow row1 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(0));
        IndexRow row2 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(0));
        IndexRow row3 = serializer.serializeRow(new Object[]{ 2, "bar" }, new RowId(0));

        assertThat(indexStorage.get(row1.indexColumns()), is(empty()));
        assertThat(indexStorage.get(row2.indexColumns()), is(empty()));
        assertThat(indexStorage.get(row3.indexColumns()), is(empty()));

        indexStorage.put(row1);
        indexStorage.put(row2);
        indexStorage.put(row3);

        assertThat(indexStorage.get(row1.indexColumns()), containsInAnyOrder(row1.rowId(), row2.rowId()));
        assertThat(indexStorage.get(row2.indexColumns()), containsInAnyOrder(row1.rowId(), row2.rowId()));
        assertThat(indexStorage.get(row3.indexColumns()), contains(row3.rowId()));
    }

    /**
     * Tests that {@link HashIndexStorage#put} does not create row ID duplicates.
     */
    @Test
    void testPutIdempotence() {
        IndexRow row = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(0));

        indexStorage.put(row);
        indexStorage.put(row);

        assertThat(indexStorage.get(row.indexColumns()), contains(row.rowId()));
    }

    /**
     * Tests the {@link HashIndexStorage#remove} method.
     */
    @Test
    void testRemove() {
        IndexRow row1 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(0));
        IndexRow row2 = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(0));
        IndexRow row3 = serializer.serializeRow(new Object[]{ 2, "bar" }, new RowId(0));

        indexStorage.put(row1);
        indexStorage.put(row2);
        indexStorage.put(row3);

        assertThat(indexStorage.get(row1.indexColumns()), containsInAnyOrder(row1.rowId(), row2.rowId()));
        assertThat(indexStorage.get(row2.indexColumns()), containsInAnyOrder(row1.rowId(), row2.rowId()));
        assertThat(indexStorage.get(row3.indexColumns()), contains(row3.rowId()));

        indexStorage.remove(row1);

        assertThat(indexStorage.get(row1.indexColumns()), contains(row2.rowId()));
        assertThat(indexStorage.get(row2.indexColumns()), contains(row2.rowId()));
        assertThat(indexStorage.get(row3.indexColumns()), contains(row3.rowId()));

        indexStorage.remove(row2);

        assertThat(indexStorage.get(row1.indexColumns()), is(empty()));
        assertThat(indexStorage.get(row2.indexColumns()), is(empty()));
        assertThat(indexStorage.get(row3.indexColumns()), contains(row3.rowId()));

        indexStorage.remove(row3);

        assertThat(indexStorage.get(row1.indexColumns()), is(empty()));
        assertThat(indexStorage.get(row2.indexColumns()), is(empty()));
        assertThat(indexStorage.get(row3.indexColumns()), is(empty()));
    }

    /**
     * Tests that {@link HashIndexStorage#remove} works normally when removing a non-existent row.
     */
    @Test
    void testRemoveIdempotence() {
        IndexRow row = serializer.serializeRow(new Object[]{ 1, "foo" }, new RowId(0));

        assertDoesNotThrow(() -> indexStorage.remove(row));

        indexStorage.put(row);

        indexStorage.remove(row);

        assertThat(indexStorage.get(row.indexColumns()), is(empty()));

        assertDoesNotThrow(() -> indexStorage.remove(row));
    }
}
