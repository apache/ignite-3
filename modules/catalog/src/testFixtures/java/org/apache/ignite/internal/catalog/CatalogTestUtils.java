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

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.SnapshotEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLog.OnUpdateHandler;
import org.apache.ignite.internal.catalog.storage.UpdateLogEvent;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.sql.ColumnType;

/** Utilities for working with the catalog in tests. */
public class CatalogTestUtils {
    private static final IgniteLogger LOG = Loggers.forClass(CatalogTestUtils.class);

    public static final int TEST_DELAY_DURATION = 0;

    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals it needs, may change in the future.
     *
     * @param nodeName Node name.
     * @param clock Hybrid clock.
     */
    public static CatalogManager createTestCatalogManager(String nodeName, HybridClock clock) {
        return createTestCatalogManager(nodeName, clock, () -> TEST_DELAY_DURATION);
    }

    /**
     * Creates a test implementation of {@link CatalogManager}.
     */
    public static CatalogManager createTestCatalogManager(String nodeName, HybridClock clock, LongSupplier delayDurationMsSupplier) {
        StandaloneMetaStorageManager metastore = StandaloneMetaStorageManager.create(nodeName, clock);

        ScheduledExecutorService scheduledExecutor = createScheduledExecutorService(nodeName);

        var clockWaiter = new ClockWaiter(nodeName, clock, scheduledExecutor);

        ClockService clockService = new TestClockService(clock, clockWaiter);

        FailureProcessor failureProcessor = new NoOpFailureManager();
        UpdateLogImpl updateLog = new UpdateLogImpl(metastore, failureProcessor);

        return new CatalogManagerImpl(
                updateLog,
                clockService,
                failureProcessor,
                delayDurationMsSupplier,
                PartitionCountCalculator.fixedPartitionCountCalculator()
        ) {
            @Override
            public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
                assertThat(metastore.startAsync(componentContext), willCompleteSuccessfully());
                assertThat(metastore.recoveryFinishedFuture(), willCompleteSuccessfully());

                return allOf(
                        clockWaiter.startAsync(componentContext),
                        super.startAsync(componentContext)
                ).thenComposeAsync(unused -> metastore.deployWatches(), componentContext.executor());
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                clockWaiter.beforeNodeStop();
                metastore.beforeNodeStop();
            }

            @Override
            public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
                return IgniteUtils.stopAsync(
                        () -> super.stopAsync(componentContext),
                        () -> clockWaiter.stopAsync(componentContext),
                        () -> shutdownAsync(scheduledExecutor),
                        () -> metastore.stopAsync(componentContext)
                );
            }
        };
    }

    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals it needs, may change in the future.
     *
     * @param nodeName Node name.
     * @param clockWaiter Clock waiter.
     * @param clock Hybrid clock.
     */
    public static CatalogManager createTestCatalogManager(String nodeName, ClockWaiter clockWaiter, HybridClock clock) {
        StandaloneMetaStorageManager metastore = StandaloneMetaStorageManager.create(nodeName);

        FailureProcessor failureProcessor = mock(FailureProcessor.class);

        return new CatalogManagerImpl(
                new UpdateLogImpl(metastore, failureProcessor),
                new TestClockService(clock, clockWaiter),
                failureProcessor,
                () -> TEST_DELAY_DURATION,
                PartitionCountCalculator.fixedPartitionCountCalculator()
        ) {
            @Override
            public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
                return allOf(metastore.startAsync(componentContext), super.startAsync(componentContext))
                        .thenComposeAsync(unused -> metastore.deployWatches(), componentContext.executor());
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                metastore.beforeNodeStop();
            }

            @Override
            public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
                return IgniteUtils.stopAsync(
                        () -> super.stopAsync(componentContext),
                        () -> metastore.stopAsync(componentContext)
                );
            }
        };
    }

    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals it needs, may change in the future.
     *
     * @param metastore Meta Storage.
     * @param clockWaiter Clock waiter.
     * @param clock Hybrid clock.
     */
    public static CatalogManager createTestCatalogManager(
            MetaStorageManager metastore,
            ClockWaiter clockWaiter,
            HybridClock clock
    ) {
        var failureProcessor = new NoOpFailureManager();

        return new CatalogManagerImpl(
                new UpdateLogImpl(metastore, failureProcessor),
                new TestClockService(clock, clockWaiter),
                failureProcessor,
                () -> TEST_DELAY_DURATION,
                PartitionCountCalculator.fixedPartitionCountCalculator()
        );
    }

    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals it needs, may change in the future.
     *
     * @param nodeName Node name.
     * @param clock Hybrid clock.
     * @param metastore Meta storage manager.
     */
    public static CatalogManager createTestCatalogManager(String nodeName, HybridClock clock, MetaStorageManager metastore) {
        return createTestCatalogManager(nodeName, clock, metastore, () -> TEST_DELAY_DURATION, () -> null);
    }

    /**
     * Creates a test implementation of {@link CatalogManager}.
     */
    public static CatalogManager createTestCatalogManager(
            String nodeName,
            HybridClock clock,
            MetaStorageManager metastore,
            LongSupplier delayDurationMsSupplier,
            Supplier<Catalog> fakeCatalogSupplier
    ) {
        ScheduledExecutorService scheduledExecutor = createScheduledExecutorService(nodeName);

        var clockWaiter = new ClockWaiter(nodeName, clock, scheduledExecutor);

        var failureProcessor = new NoOpFailureManager();

        return new CatalogManagerImpl(
                new UpdateLogImpl(metastore, failureProcessor),
                new TestClockService(clock, clockWaiter),
                failureProcessor,
                delayDurationMsSupplier,
                PartitionCountCalculator.fixedPartitionCountCalculator()
        ) {
            @Override
            public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
                return allOf(clockWaiter.startAsync(componentContext), super.startAsync(componentContext));
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                clockWaiter.beforeNodeStop();
            }

            @Override
            public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
                return IgniteUtils.stopAsync(
                        () -> super.stopAsync(componentContext),
                        () -> clockWaiter.stopAsync(componentContext),
                        () -> shutdownAsync(scheduledExecutor)
                );
            }

            @Override
            public Catalog catalog(int catalogVersion) {
                Catalog fakeCatalog = fakeCatalogSupplier.get();
                return fakeCatalog == null ? super.catalog(catalogVersion) : fakeCatalog;
            }

            @Override
            public Catalog earliestCatalog() {
                Catalog fakeCatalog = fakeCatalogSupplier.get();
                return fakeCatalog == null ? super.earliestCatalog() : fakeCatalog;
            }
        };
    }

    /**
     * Creates a test implementation of {@link CatalogManager}.
     *
     * <p>NOTE: Uses {@link CatalogManagerImpl} under the hood and creates the internals it needs, may change in the future.
     *
     * @param nodeName Node name.
     * @param clock Hybrid clock.
     * @param metastore Meta storage manager.
     */
    public static CatalogManager createTestCatalogManagerWithInterceptor(
            String nodeName,
            HybridClock clock,
            MetaStorageManager metastore,
            UpdateHandlerInterceptor interceptor
    ) {
        ScheduledExecutorService scheduledExecutor = createScheduledExecutorService(nodeName);

        var clockWaiter = new ClockWaiter(nodeName, clock, scheduledExecutor);

        var failureProcessor = new NoOpFailureManager();

        UpdateLogImpl updateLog = new UpdateLogImpl(metastore, failureProcessor) {
            @Override
            public void registerUpdateHandler(OnUpdateHandler handler) {
                interceptor.registerUpdateHandler(handler);

                super.registerUpdateHandler(interceptor);
            }
        };

        return new CatalogManagerImpl(
                updateLog,
                new TestClockService(clock, clockWaiter),
                failureProcessor,
                () -> TEST_DELAY_DURATION,
                PartitionCountCalculator.fixedPartitionCountCalculator()
        ) {
            @Override
            public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
                return allOf(clockWaiter.startAsync(componentContext), super.startAsync(componentContext));
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                clockWaiter.beforeNodeStop();
            }

            @Override
            public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
                return IgniteUtils.stopAsync(
                        () -> super.stopAsync(componentContext),
                        () -> clockWaiter.stopAsync(componentContext),
                        () -> shutdownAsync(scheduledExecutor)
                );
            }
        };
    }

    /**
     * Create the same {@link CatalogManager} as for normal operations, but with {@link UpdateLog} that
     * simply notifies the manager without storing any updates in metastore.
     *
     * <p>Particular configuration of manager pretty fast (in terms of awaiting of certain safe time) and lightweight.
     * It doesn't contain any mocks from {@link org.mockito.Mockito}.
     *
     * @param nodeName Name of the node that is meant to own this manager. Any thread spawned by returned instance
     *      will have it as thread's name prefix.
     * @param clock This clock is used to assign activation timestamp for incoming updates, thus make it possible
     *      to acquired schema that was valid at give time.
     * @return An instance of {@link CatalogManager catalog manager}.
     */
    public static CatalogManager createCatalogManagerWithTestUpdateLog(String nodeName, HybridClock clock) {
        ScheduledExecutorService scheduledExecutor = createScheduledExecutorService(nodeName);

        var clockWaiter = new ClockWaiter(nodeName, clock, scheduledExecutor);

        return new CatalogManagerImpl(
                new TestUpdateLog(clock),
                new TestClockService(clock, clockWaiter),
                new NoOpFailureManager(),
                () -> TEST_DELAY_DURATION,
                PartitionCountCalculator.fixedPartitionCountCalculator()
        ) {
            @Override
            public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
                return allOf(clockWaiter.startAsync(componentContext), super.startAsync(componentContext));
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();

                clockWaiter.beforeNodeStop();
            }

            @Override
            public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
                return IgniteUtils.stopAsync(
                        () -> super.stopAsync(componentContext),
                        () -> clockWaiter.stopAsync(componentContext),
                        () -> shutdownAsync(scheduledExecutor)
                );
            }
        };
    }

    /** Default nullable behavior. */
    public static final boolean DEFAULT_NULLABLE = false;

    /** Append precision\scale according to type requirement. */
    public static Builder initializeColumnWithDefaults(ColumnType type, Builder colBuilder) {
        if (type.precisionAllowed()) {
            colBuilder.precision(4);
        }

        if (type.scaleAllowed()) {
            colBuilder.scale(0);
        }

        if (type.lengthAllowed()) {
            colBuilder.length(1 << 5);
        }

        return colBuilder;
    }

    /** Append precision according to type requirement. */
    public static void applyNecessaryPrecision(ColumnType type, Builder colBuilder) {
        if (type.precisionAllowed()) {
            colBuilder.precision(11);
        }
    }

    /** Append length according to type requirement. */
    public static void applyNecessaryLength(ColumnType type, Builder colBuilder) {
        if (type.lengthAllowed()) {
            colBuilder.length(1 << 5);
        }
    }

    public static ColumnParams columnParams(String name, ColumnType type) {
        return columnParams(name, type, DEFAULT_NULLABLE);
    }

    public static ColumnParams columnParams(String name, ColumnType type, boolean nullable) {
        return columnParamsBuilder(name, type, nullable).build();
    }

    static ColumnParams columnParams(String name, ColumnType type, boolean nullable, int precision) {
        return columnParamsBuilder(name, type, nullable, precision).build();
    }

    static ColumnParams columnParams(String name, ColumnType type, boolean nullable, int precision, int scale) {
        return columnParamsBuilder(name, type, nullable, precision, scale).build();
    }

    static ColumnParams columnParams(String name, ColumnType type, int length, boolean nullable) {
        return columnParamsBuilder(name, type, length, nullable).build();
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type) {
        return columnParamsBuilder(name, type, DEFAULT_NULLABLE);
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, boolean nullable) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type);
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, boolean nullable, int precision) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type).precision(precision);
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, boolean nullable, int precision, int scale) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type).precision(precision).scale(scale);
    }

    static ColumnParams.Builder columnParamsBuilder(String name, ColumnType type, int length, boolean nullable) {
        return ColumnParams.builder().name(name).nullable(nullable).type(type).length(length);
    }

    static CatalogCommand dropTableCommand(String tableName) {
        return DropTableCommand.builder().schemaName(SqlCommon.DEFAULT_SCHEMA_NAME).tableName(tableName).build();
    }

    static CatalogCommand dropColumnParams(String tableName, String... columns) {
        return AlterTableDropColumnCommand.builder()
                .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .tableName(tableName)
                .columns(Set.of(columns))
                .build();
    }

    static CatalogCommand addColumnParams(String tableName, ColumnParams... columns) {
        return AlterTableAddColumnCommand.builder()
                .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                .tableName(tableName)
                .columns(List.of(columns))
                .build();
    }

    /**
     * Builder for {@link CreateZoneCommand}.
     *
     * @param zoneName Zone name.
     * @return Builder for {@link CreateZoneCommand}.
     */
    public static CreateZoneCommandBuilder createZoneBuilder(String zoneName) {
        return CreateZoneCommand.builder()
                .zoneName(zoneName)
                .storageProfilesParams(List.of(StorageProfileParams.builder().storageProfile(DEFAULT_STORAGE_PROFILE).build()));
    }

    public static AlterZoneCommandBuilder alterZoneBuilder(String zoneName) {
        return AlterZoneCommand.builder().zoneName(zoneName);
    }

    private static class TestUpdateLog implements UpdateLog {
        private final HybridClock clock;

        private long lastSeenVersion = 0;
        private long snapshotVersion = 0;

        private volatile OnUpdateHandler onUpdateHandler;

        private TestUpdateLog(HybridClock clock) {
            this.clock = clock;
        }

        @Override
        public synchronized CompletableFuture<Boolean> append(VersionedUpdate update) {
            if (update.version() - 1 != lastSeenVersion) {
                return falseCompletedFuture();
            }

            lastSeenVersion = update.version();

            return onUpdateHandler.handle(update, clock.now(), update.version()).thenApply(ignored -> true);
        }

        @Override
        public synchronized CompletableFuture<Boolean> saveSnapshot(SnapshotEntry snapshotEntry) {
            snapshotVersion = snapshotEntry.version();
            return trueCompletedFuture();
        }

        @Override
        public void registerUpdateHandler(OnUpdateHandler handler) {
            this.onUpdateHandler = handler;
        }

        @Override
        public CompletableFuture<Void> startAsync(ComponentContext componentContext) throws IgniteInternalException {
            if (onUpdateHandler == null) {
                throw new IgniteInternalException(
                        Common.INTERNAL_ERR,
                        "Handler must be registered prior to component start"
                );
            }

            lastSeenVersion = snapshotVersion;

            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
            return nullCompletedFuture();
        }
    }

    /**
     * Searches for a table by name in the requested version of the catalog.
     *
     * @param catalogService Catalog service.
     * @param catalogVersion Catalog version in which to find the table.
     * @param tableName Table name.
     */
    public static CatalogTableDescriptor table(CatalogService catalogService, int catalogVersion, String tableName) {
        CatalogTableDescriptor tableDescriptor = catalogService.catalog(catalogVersion).tables().stream()
                .filter(table -> tableName.equals(table.name()))
                .findFirst()
                .orElse(null);

        assertNotNull(tableDescriptor, "catalogVersion=" + catalogVersion + ", tableName=" + tableName);

        return tableDescriptor;
    }

    /**
     * Update handler interceptor for test purposes.
     */
    public abstract static class UpdateHandlerInterceptor implements OnUpdateHandler {
        private OnUpdateHandler delegate;

        void registerUpdateHandler(OnUpdateHandler handler) {
            this.delegate = handler;
        }

        protected OnUpdateHandler delegate() {
            return delegate;
        }
    }

    /**
     * An interceptor, which allow dropping events.
     */
    public static class TestUpdateHandlerInterceptor extends UpdateHandlerInterceptor {
        private volatile boolean dropEvents;

        public void dropSnapshotEvents() {
            this.dropEvents = true;
        }

        @Override
        public CompletableFuture<Void> handle(UpdateLogEvent update, HybridTimestamp metaStorageUpdateTimestamp, long causalityToken) {
            if (dropEvents && update instanceof SnapshotEntry) {
                return nullCompletedFuture();
            }

            return delegate().handle(update, metaStorageUpdateTimestamp, causalityToken);
        }
    }

    /** Test command that does nothing but increments object id counter of a catalog. */
    public static class TestCommand implements CatalogCommand {

        private final Boolean successful;

        private TestCommand(Boolean successful) {
            this.successful = successful;
        }

        public static TestCommand ok() {
            return new TestCommand(true);
        }

        public static TestCommand fail() {
            return new TestCommand(false);
        }

        public static TestCommand empty() {
            return new TestCommand(null);
        }

        @Override
        public List<UpdateEntry> get(UpdateContext updateContext) {
            if (successful == null) {
                return List.of();
            }
            if (!successful) {
                throw new TestCommandFailure();
            }
            return List.of(new ObjectIdGenUpdateEntry(1));
        }
    }

    /** Test command failure. */
    public static class TestCommandFailure extends RuntimeException {
        private static final long serialVersionUID = -6123535862914825943L;
    }

    private static ScheduledExecutorService createScheduledExecutorService(String nodeName) {
        return Executors.newSingleThreadScheduledExecutor(
                IgniteThreadFactory.create(nodeName, "catalog-utils-scheduled-executor", LOG)
        );
    }

    private static CompletableFuture<Void> shutdownAsync(ScheduledExecutorService scheduledExecutorService) {
        return runAsync(() -> IgniteUtils.shutdownAndAwaitTermination(scheduledExecutorService, 10, TimeUnit.SECONDS));
    }
}
