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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.catalog.CatalogTestUtils.awaitDefaultZoneCreation;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParamsBuilder;
import static org.apache.ignite.internal.catalog.commands.DefaultValue.constant;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.apache.ignite.sql.ColumnType.DECIMAL;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.spy;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.commands.TablePrimaryKey;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for testing the {@link CatalogManager}.
 */
public abstract class BaseCatalogManagerTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "test";

    protected static final String TABLE_NAME = "test_table";
    protected static final String TABLE_NAME_2 = "test_table_2";
    protected static final String TABLE_NAME_3 = "test_table_3";

    protected static final String INDEX_NAME = "myIndex";
    protected static final String INDEX_NAME_2 = "myIndex2";

    protected final HybridClock clock = new HybridClockImpl();

    ClockWaiter clockWaiter;

    ClockService clockService;

    private MetaStorageManager metastore;

    UpdateLog updateLog;

    protected CatalogManagerImpl manager;

    protected AtomicLong delayDuration = new AtomicLong();
    protected AtomicLong partitionIdleSafeTimePropagationPeriod = new AtomicLong();


    @BeforeEach
    void setUp() {
        delayDuration.set(CatalogManagerImpl.DEFAULT_DELAY_DURATION);
        partitionIdleSafeTimePropagationPeriod.set(CatalogManagerImpl.DEFAULT_PARTITION_IDLE_SAFE_TIME_PROPAGATION_PERIOD);

        metastore = StandaloneMetaStorageManager.create(new SimpleInMemoryKeyValueStorage(NODE_NAME));

        updateLog = spy(new UpdateLogImpl(metastore));
        clockWaiter = spy(new ClockWaiter(NODE_NAME, clock));

        clockService = new TestClockService(clock, clockWaiter);

        manager = new CatalogManagerImpl(
                updateLog,
                clockService,
                delayDuration::get,
                partitionIdleSafeTimePropagationPeriod::get
        );

        assertThat(startAsync(ForkJoinPool.commonPool(), metastore, clockWaiter, manager), willCompleteSuccessfully());

        assertThat("Watches were not deployed", metastore.deployWatches(), willCompleteSuccessfully());

        awaitDefaultZoneCreation(manager);
    }

    @AfterEach
    public void tearDown() {
        assertThat(stopAsync(manager, clockWaiter, metastore), willCompleteSuccessfully());
    }

    protected void createSomeTable(String tableName) {
        assertThat(
                manager.execute(createTableCommand(
                        tableName,
                        List.of(columnParams("key1", INT32), columnParams("val1", INT32)),
                        List.of("key1"),
                        List.of("key1")
                )),
                willCompleteSuccessfully()
        );
    }

    protected final Catalog latestActiveCatalog() {
        Catalog catalog = manager.catalog(manager.activeCatalogVersion(clock.nowLong()));

        return Objects.requireNonNull(catalog);
    }

    protected static CatalogCommand createHashIndexCommand(
            String tableName,
            String indexName,
            boolean uniq,
            @Nullable List<String> indexColumns
    ) {
        return CreateHashIndexCommand.builder()
                .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .tableName(tableName)
                .indexName(indexName)
                .unique(uniq)
                .columns(indexColumns)
                .build();
    }

    protected static CatalogCommand createHashIndexCommand(
            String indexName,
            boolean uniq,
            @Nullable List<String> indexColumns
    ) {
        return createHashIndexCommand(TABLE_NAME, indexName, uniq, indexColumns);
    }

    protected static CatalogCommand createHashIndexCommand(
            String indexName,
            @Nullable List<String> indexColumns
    ) {
        return createHashIndexCommand(indexName, false, indexColumns);
    }

    protected static CatalogCommand createSortedIndexCommand(
            String indexName,
            boolean unique,
            @Nullable List<String> indexColumns,
            @Nullable List<CatalogColumnCollation> columnsCollations
    ) {
        return createSortedIndexCommand(TABLE_NAME, indexName, unique, indexColumns, columnsCollations);
    }

    protected static CatalogCommand createSortedIndexCommand(
            String tableName,
            String indexName,
            boolean unique,
            @Nullable List<String> indexColumns,
            @Nullable List<CatalogColumnCollation> columnsCollations
    ) {
        return CreateSortedIndexCommand.builder()
                .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .tableName(tableName)
                .indexName(indexName)
                .unique(unique)
                .columns(indexColumns)
                .collations(columnsCollations)
                .build();
    }

    protected static CatalogCommand createSortedIndexCommand(
            String indexName,
            @Nullable List<String> indexColumns,
            @Nullable List<CatalogColumnCollation> columnsCollations
    ) {
        return createSortedIndexCommand(indexName, false, indexColumns, columnsCollations);
    }

    protected static CatalogCommand createTableCommand(
            String tableName,
            List<ColumnParams> columns,
            List<String> primaryKeys,
            @Nullable List<String> colocationColumns
    ) {
        return createTableCommandBuilder(tableName, columns, primaryKeys, colocationColumns)
                .build();
    }

    protected static CreateTableCommandBuilder createTableCommandBuilder(String tableName,
            List<ColumnParams> columns,
            List<String> primaryKeys, @Nullable List<String> colocationColumns) {

        TablePrimaryKey primaryKey = TableHashPrimaryKey.builder()
                .columns(primaryKeys)
                .build();

        return CreateTableCommand.builder()
                .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .tableName(tableName)
                .columns(columns)
                .primaryKey(primaryKey)
                .colocationColumns(colocationColumns);
    }

    protected static CatalogCommand simpleTable(String tableName) {
        List<ColumnParams> cols = List.of(
                columnParams("ID", INT32),
                columnParamsBuilder("VAL", INT32, true).defaultValue(constant(null)).build(),
                columnParamsBuilder("VAL_NOT_NULL", INT32).defaultValue(constant(1)).build(),
                columnParams("DEC", DECIMAL, true, 11, 2),
                columnParams("STR", STRING, 101, true),
                columnParamsBuilder("DEC_SCALE", DECIMAL).precision(12).scale(3).build()
        );

        return simpleTable(tableName, cols);
    }

    protected static CatalogCommand simpleTable(String tableName, List<ColumnParams> cols) {
        return createTableCommand(tableName, cols, List.of(cols.get(0).name()), List.of(cols.get(0).name()));
    }

    protected static CatalogCommand simpleIndex() {
        return createSortedIndexCommand(INDEX_NAME, List.of("VAL"), List.of(ASC_NULLS_LAST));
    }

    protected static CatalogCommand simpleIndex(String tableName, String indexName) {
        return createHashIndexCommand(tableName, indexName, false, List.of("VAL_NOT_NULL"));
    }

    protected static CatalogCommand startBuildingIndexCommand(int indexId) {
        return StartBuildingIndexCommand.builder().indexId(indexId).build();
    }

    protected static CatalogCommand dropTableCommand(String tableName) {
        return DropTableCommand.builder()
                .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .tableName(tableName)
                .build();
    }

    protected static CatalogCommand dropIndexCommand(String indexName) {
        return DropIndexCommand.builder()
                .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .indexName(indexName)
                .build();
    }

    protected static <T extends CatalogEventParameters> EventListener<T> fromConsumer(
            CompletableFuture<Void> fireEventFuture,
            Consumer<T> consumer
    ) {
        return parameters -> {
            try {
                consumer.accept(parameters);

                fireEventFuture.complete(null);
            } catch (Throwable t) {
                fireEventFuture.completeExceptionally(t);
            }

            return falseCompletedFuture();
        };
    }
}
