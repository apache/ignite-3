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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.TEST_DELAY_DURATION;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParams;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParamsBuilder;
import static org.apache.ignite.internal.catalog.commands.DefaultValue.constant;
import static org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation.ASC_NULLS_LAST;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.apache.ignite.sql.ColumnType.DECIMAL;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.spy;

import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.commands.RenameIndexCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.commands.TablePrimaryKey;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for testing the {@link CatalogManager}.
 */
@ExtendWith(ExecutorServiceExtension.class)
public abstract class BaseCatalogManagerTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "test";

    protected static final String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;

    protected static final String ZONE_NAME = "test_zone";

    protected static final String TABLE_NAME = "test_table";
    protected static final String TABLE_NAME_2 = "test_table_2";
    protected static final String TABLE_NAME_3 = "test_table_3";

    protected static final String INDEX_NAME = "myIndex";
    protected static final String INDEX_NAME_2 = "myIndex2";

    protected final HybridClock clock = new HybridClockImpl();

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutor;

    ClockWaiter clockWaiter;

    ClockService clockService;

    protected MetaStorageManager metastore;

    UpdateLog updateLog;

    protected CatalogManagerImpl manager;

    protected AtomicLong delayDuration = new AtomicLong(TEST_DELAY_DURATION);

    @BeforeEach
    void setUp() {
        metastore = StandaloneMetaStorageManager.create(NODE_NAME, clock);

        FailureProcessor failureProcessor = new NoOpFailureManager();
        updateLog = spy(new UpdateLogImpl(metastore, failureProcessor));
        clockWaiter = spy(new ClockWaiter(NODE_NAME, clock, scheduledExecutor));

        clockService = new TestClockService(clock, clockWaiter);

        manager = new CatalogManagerImpl(
                updateLog,
                clockService,
                failureProcessor,
                delayDuration::get,
                PartitionCountProvider.defaultPartitionCountProvider()
        );

        ComponentContext context = new ComponentContext();
        assertThat(startAsync(context, metastore), willCompleteSuccessfully());
        assertThat(metastore.recoveryFinishedFuture(), willCompleteSuccessfully());

        assertThat(startAsync(context, clockWaiter, manager), willCompleteSuccessfully());

        assertThat("Watches were not deployed", metastore.deployWatches(), willCompleteSuccessfully());

        await(manager.catalogInitializationFuture());
    }

    @AfterEach
    public void tearDown() {
        assertThat(stopAsync(new ComponentContext(), manager, clockWaiter, metastore), willCompleteSuccessfully());
    }

    protected void createSomeTable(String tableName) {
        tryApplyAndExpectApplied(createTableCommand(
                tableName,
                List.of(columnParams("key1", INT32), columnParams("val1", INT32)),
                List.of("key1"),
                List.of("key1")
        ));
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
        return createSortedIndexCommand(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME, indexName, unique, indexColumns, columnsCollations);
    }

    protected static CatalogCommand createSortedIndexCommand(
            String schemaName,
            String tableName,
            String indexName,
            boolean unique,
            @Nullable List<String> indexColumns,
            @Nullable List<CatalogColumnCollation> columnsCollations
    ) {
        return CreateSortedIndexCommand.builder()
                .schemaName(schemaName)
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
        return createTableCommand(SqlCommon.DEFAULT_SCHEMA_NAME, tableName, columns, primaryKeys, colocationColumns);
    }

    protected static CatalogCommand createTableCommand(
            String schemaName,
            String tableName,
            List<ColumnParams> columns,
            List<String> primaryKeys,
            @Nullable List<String> colocationColumns
    ) {
        return createTableCommandBuilder(schemaName, tableName, columns, primaryKeys, colocationColumns)
                .build();
    }

    protected static CreateTableCommandBuilder createTableCommandBuilder(
            String schemaName,
            String tableName,
            List<ColumnParams> columns,
            List<String> primaryKeys, @Nullable List<String> colocationColumns) {

        TablePrimaryKey primaryKey = TableHashPrimaryKey.builder()
                .columns(primaryKeys)
                .build();

        return CreateTableCommand.builder()
                .schemaName(schemaName)
                .tableName(tableName)
                .columns(columns)
                .primaryKey(primaryKey)
                .colocationColumns(colocationColumns);
    }

    protected static CatalogCommand createZoneCommand(
            String zoneName
    ) {
        return CreateZoneCommand.builder()
                .zoneName(zoneName)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()))
                .build();
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

    protected static CatalogCommand simpleZone(String zoneName) {
        return createZoneCommand(zoneName);
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

    protected static CatalogCommand renameIndexCommand(String indexName, String newIndexName) {
        return RenameIndexCommand.builder()
                .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .indexName(indexName)
                .newIndexName(newIndexName)
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

    CatalogApplyResult tryApplyAndExpectApplied(CatalogCommand cmd) {
        return tryApplyAndCheckExpect(List.of(cmd), true);
    }

    CatalogApplyResult tryApplyAndExpectNotApplied(CatalogCommand cmd) {
        return tryApplyAndCheckExpect(List.of(cmd), false);
    }

    CatalogApplyResult tryApplyAndCheckExpect(List<CatalogCommand> cmds, boolean... expectedResults) {
        CatalogApplyResult applyResult = await(manager.execute(cmds));
        assertThat(applyResult, CatalogApplyResultMatcher.fewResults(expectedResults));

        return applyResult;
    }

    static <T> CompletableFutureMatcher<T> willBeApplied() {
        return (CompletableFutureMatcher<T>) willBe(CatalogApplyResultMatcher.applyed());
    }

    static <T> CompletableFutureMatcher<T> willBeNotApplied() {
        return (CompletableFutureMatcher<T>) willBe(CatalogApplyResultMatcher.notApplyed());
    }

    static class CatalogApplyResultMatcher extends BaseMatcher<CatalogApplyResult> {
        BitSet expectedResult;

        CatalogApplyResultMatcher(boolean expectedSingleResult) {
            expectedResult = new BitSet(1);
            expectedResult.set(0, expectedSingleResult);
        }

        CatalogApplyResultMatcher(BitSet expectedResult) {
            this.expectedResult = expectedResult;
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof CatalogApplyResult) {
                CatalogApplyResult catalogApplyResult = (CatalogApplyResult) o;
                for (int i = 0; i < expectedResult.size(); i++) {
                    if (catalogApplyResult.isApplied(i) != expectedResult.get(i)) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("CatalogApplyResultMatcher: " + expectedResult.toString());
        }

        static CatalogApplyResultMatcher fewResults(boolean... expectedResult) {
            BitSet bitSet = new BitSet(expectedResult.length);
            for (int i = 0; i < expectedResult.length; i++) {
                bitSet.set(i, expectedResult[i]);
            }

            return new CatalogApplyResultMatcher(bitSet);
        }

        static CatalogApplyResultMatcher applyed() {
            return new CatalogApplyResultMatcher(true);
        }

        static CatalogApplyResultMatcher notApplyed() {
            return new CatalogApplyResultMatcher(false);
        }
    }
}
