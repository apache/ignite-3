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

package org.apache.ignite.internal.sql.engine.framework;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STALE_ROWS_FRACTION;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImplTest.PLANNING_THREAD_COUNT;
import static org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImplTest.PLAN_EXPIRATION_SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTable;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistry;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.ExecutionId;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.UpdatableTable;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionDistributionProvider;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPrunerImpl;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptorImpl;
import org.apache.ignite.internal.sql.engine.schema.DefaultValueStrategy;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.IgniteTableImpl;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptorImpl;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManager;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.table.distributed.TableStatsStalenessConfiguration;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.internal.util.TransformingIterator;
import org.apache.ignite.internal.util.subscription.TransformingPublisher;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * A collection of builders to create test objects.
 */
public class TestBuilders {
    private static final int ZONE_ID = 10000;

    private static final AtomicInteger TABLE_ID_GEN = new AtomicInteger();

    private static final IgniteLogger LOG = Loggers.forClass(TestBuilders.class);

    /** Returns a builder of the test cluster object. */
    public static ClusterBuilder cluster() {
        return new ClusterBuilderImpl();
    }

    /** Returns a builder of the test table object. */
    public static TableBuilder table() {
        return new TableBuilderImpl();
    }

    /** Returns a builder of the execution context. */
    public static ExecutionContextBuilder executionContext() {
        return new ExecutionContextBuilderImpl();
    }

    /** Factory method to create a cluster service factory for cluster consisting of provided nodes. */
    public static ClusterServiceFactory clusterServiceFactory(List<String> nodes) {
        var logicalTopology = logicalTopology(nodes);

        return new ClusterServiceFactory(logicalTopology);
    }

    /** Factory method to create a logical topology from provided nodes. */
    public static LogicalTopology logicalTopology(Collection<String> nodes) {
        var logicalTopology = new LogicalTopologyImpl(
                TestClusterStateStorage.initializedClusterStateStorage(),
                ctx -> false
        );

        createLogicalNodes(nodes, Map.of()).forEach(logicalTopology::putNode);

        return logicalTopology;
    }

    /**
     * Factory method to create {@link ScannableTable table} instance from given data provider with only implemented
     * {@link ScannableTable#scan table scan}.
     */
    public static ScannableTable tableScan(DataProvider<Object[]> dataProvider) {
        return new AbstractScannableTable() {
            @Override
            public <RowT> Publisher<RowT> scan(
                    ExecutionContext<RowT> ctx,
                    PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory,
                    int @Nullable [] requiredColumns
            ) {

                return new TransformingPublisher<>(
                        SubscriptionUtils.fromIterable(
                                () -> new TransformingIterator<>(
                                        dataProvider.iterator(),
                                        row -> project(row, requiredColumns)
                                )
                        ),
                        rowFactory::create
                );
            }
        };
    }

    /**
     * Factory method to create {@link ScannableTable table} instance from given data provider with only implemented
     * {@link ScannableTable#scan table scan}.
     */
    public static ScannableTable tableScan(BiFunction<String, Integer, Iterable<Object[]>> generatorFunction) {
        return new AbstractScannableTable() {
            @Override
            public <RowT> Publisher<RowT> scan(
                    ExecutionContext<RowT> ctx,
                    PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory,
                    int @Nullable [] requiredColumns
            ) {

                return new TransformingPublisher<>(
                        SubscriptionUtils.fromIterable(
                                () -> new TransformingIterator<>(
                                        generatorFunction.apply(ctx.localNode().name(), partWithConsistencyToken.partId()).iterator(),
                                        row -> project(row, requiredColumns)
                                )
                        ),
                        rowFactory::create
                );
            }
        };
    }

    /**
     * Factory method to create {@link ScannableTable table} instance from given data provider with only implemented
     * {@link ScannableTable#indexRangeScan index range scan}.
     */
    public static ScannableTable indexRangeScan(DataProvider<Object[]> dataProvider) {
        return new AbstractScannableTable() {
            @Override
            public <RowT> Publisher<RowT> indexRangeScan(
                    ExecutionContext<RowT> ctx,
                    PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory,
                    int indexId,
                    List<String> columns,
                    @Nullable RangeCondition<RowT> cond,
                    int @Nullable [] requiredColumns
            ) {
                return new TransformingPublisher<>(
                        SubscriptionUtils.fromIterable(
                                () -> new TransformingIterator<>(
                                        dataProvider.iterator(),
                                        row -> project(row, requiredColumns)
                                )
                        ),
                        rowFactory::create
                );
            }
        };
    }

    /**
     * Factory method to create {@link ScannableTable table} instance from given data provider with only implemented
     * {@link ScannableTable#indexLookup index lookup}.
     */
    public static ScannableTable indexLookup(DataProvider<Object[]> dataProvider) {
        return new AbstractScannableTable() {
            @Override
            public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, RowT key,
                    int @Nullable [] requiredColumns) {
                return new TransformingPublisher<>(
                        SubscriptionUtils.fromIterable(
                                () -> new TransformingIterator<>(
                                        dataProvider.iterator(),
                                        row -> project(row, requiredColumns)
                                )
                        ),
                        rowFactory::create
                );
            }
        };
    }

    /**
     * Creates an affinity distribution that takes into account the zone ID and calculates the destinations
     * based on a hash function which takes into account the key field types of the row.
     *
     * @param key Affinity key ordinal.
     * @param tableId Table ID.
     * @param zoneId Distribution zone ID.
     * @return Affinity distribution.
     */
    public static IgniteDistribution affinity(int key, int tableId, int zoneId) {
        return affinity(ImmutableIntList.of(key), tableId, zoneId);
    }

    /**
     * Creates an affinity distribution that takes into account the zone ID and calculates the destinations
     * based on a hash function which takes into account the key field types of the row.
     *
     * @param keys Affinity keys ordinals. Should not be null or empty.
     * @param tableId Table ID.
     * @param zoneId  Distribution zone ID.
     * @return Affinity distribution.
     */
    public static IgniteDistribution affinity(List<Integer> keys, int tableId, int zoneId) {
        return IgniteDistributions.affinity(keys, tableId, zoneId, affinityDistributionLabel(tableId, zoneId));
    }

    private static String affinityDistributionLabel(int tableId, int zoneId) {
        return format("affinity [tableId={}, zoneId={}]", tableId, zoneId);
    }

    /**
     * A builder to create a test cluster object.
     *
     * @see TestCluster
     */
    public interface ClusterBuilder {
        /**
         * Sets desired names for the cluster nodes.
         *
         * @param firstNodeName A name of the first node. There is no difference in what node should be first. This parameter was
         *         introduced to force user to provide at least one node name.
         * @param otherNodeNames An array of rest of the names to create cluster from.
         * @return {@code this} for chaining.
         */
        ClusterBuilder nodes(String firstNodeName, String... otherNodeNames);

        /**
         * A decorator to wrap {@link CatalogManager} instance which will be used in the test cluster.
         *
         * <p>May be used to slow down or ignore certain catalog commands.
         *
         * @param decorator A decorator function which accepts original manager and returns decorated one.
         * @return {@code this} for chaining.
         */
        ClusterBuilder catalogManagerDecorator(Function<CatalogManager, CatalogManager> decorator);

        /**
         * Sets desired handlers for operation cancellation.
         *
         * <p>Cannot be used to cancel operation on test cluster, but may serve as mock.
         *
         * @param handlers The handlers to set.
         * @return {@code this} for chaining.
         */
        ClusterBuilder operationKillHandlers(OperationKillHandler... handlers);

        /**
         * Adds the given system view to the cluster.
         *
         * @param systemView System view.
         * @param <T> System view data type.
         * @return {@code this} for chaining.
         */
        <T> ClusterBuilder addSystemView(SystemView<T> systemView);

        /**
         * Builds the cluster object.
         *
         * @return Created cluster object.
         */
        TestCluster build();

        /**
         * Provides implementation of table with given name.
         *
         * @param defaultDataProvider Name of the table given instance represents.
         * @return {@code this} for chaining.
         */
        ClusterBuilder defaultDataProvider(DefaultDataProvider defaultDataProvider);

        /**
         * Provides implementation of table with given name.
         *
         * @param defaultAssignmentsProvider Name of the table given instance represents.
         * @return {@code this} for chaining.
         */
        ClusterBuilder defaultAssignmentsProvider(DefaultAssignmentsProvider defaultAssignmentsProvider);

        /**
         * Registers a previously added system view (see {@link #addSystemView(SystemView)}) on the specified node.
         *
         * @param nodeName Name of the node the view is going to be registered at.
         * @param systemViewName Name of previously registered system.
         * @return {@code this} for chaining.
         */
        ClusterBuilder registerSystemView(String nodeName, String systemViewName);

        /**
         * Sets a timeout for query optimization phase.
         *
         * @param value A planning timeout value.
         * @param timeUnit A time unit.
         * @return {@code this} for chaining.
         */
        ClusterBuilder planningTimeout(long value, TimeUnit timeUnit);
    }

    /**
     * A builder to create a test table object.
     */
    public interface TableBuilder extends TableBuilderBase<TableBuilder> {
        /** Returns a builder of the test sorted-index object. */
        SortedIndexBuilder sortedIndex();

        /** Returns a builder of the test hash-index object. */
        HashIndexBuilder hashIndex();

        /** Sets the distribution of the table. */
        TableBuilder distribution(IgniteDistribution distribution);

        /** Sets the size of the table. */
        TableBuilder size(int size);

        /** Sets id for the table. The caller must guarantee that provided id is unique. */
        TableBuilder tableId(int id);

        /** Sets the number of partitions fot this table. Default value is equal to {@link CatalogUtils#DEFAULT_PARTITION_COUNT}. */
        TableBuilder partitions(int num);

        /**
         * Builds a table.
         *
         * @return Created table object.
         */
        IgniteTable build();
    }

    /**
     * A builder to create a test object that representing sorted index.
     *
     * @see TestIndex
     */
    public interface SortedIndexBuilder extends SortedIndexBuilderBase<SortedIndexBuilder>, NestedBuilder<TableBuilder> {

        /** Specifies whether this index is a primary key index or not. */
        SortedIndexBuilder primaryKey(boolean value);
    }

    /**
     * A builder to create a test object that representing hash index.
     *
     * @see TestIndex
     */
    public interface HashIndexBuilder extends HashIndexBuilderBase<HashIndexBuilder>, NestedBuilder<TableBuilder> {

        /** Specifies whether this index is a primary key index or not. */
        HashIndexBuilder primaryKey(boolean value);
    }

    /**
     * A builder to create an execution context.
     *
     * @see ExecutionContext
     */
    public interface ExecutionContextBuilder {
        /** Sets the identifier of the query. */
        ExecutionContextBuilder queryId(UUID queryId);

        /** Sets the description of the fragment this context will be created for. */
        ExecutionContextBuilder fragment(FragmentDescription fragmentDescription);

        /** Sets the query task executor. */
        ExecutionContextBuilder executor(QueryTaskExecutor executor);

        /** Sets the node this fragment will be executed on. */
        ExecutionContextBuilder localNode(InternalClusterNode node);

        /** Sets the dynamic parameters this fragment will be executed with. */
        ExecutionContextBuilder dynamicParameters(Object... params);

        /** Sets the client's timezone. */
        ExecutionContextBuilder timeZone(ZoneId zoneId);

        /** Sets the clock used to obtain the system time. */
        ExecutionContextBuilder clock(Clock clock);

        /**
         * Builds the context object.
         *
         * @return Created context object.
         */
        ExecutionContext<Object[]> build();
    }

    private static class ExecutionContextBuilderImpl implements ExecutionContextBuilder {
        private FragmentDescription description = new FragmentDescription(0, true, null, null, null, null);

        private UUID queryId = null;
        private QueryTaskExecutor executor = null;
        private InternalClusterNode node = null;
        private Object[] dynamicParams = ArrayUtils.OBJECT_EMPTY_ARRAY;
        private ZoneId zoneId = SqlCommon.DEFAULT_TIME_ZONE_ID;
        private Clock clock = Clock.systemUTC();

        /** {@inheritDoc} */
        @Override
        public ExecutionContextBuilder queryId(UUID queryId) {
            this.queryId = Objects.requireNonNull(queryId, "queryId");

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ExecutionContextBuilder fragment(FragmentDescription fragmentDescription) {
            this.description = Objects.requireNonNull(fragmentDescription, "fragmentDescription");

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ExecutionContextBuilder executor(QueryTaskExecutor executor) {
            this.executor = Objects.requireNonNull(executor, "executor");

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ExecutionContextBuilder localNode(InternalClusterNode node) {
            this.node = Objects.requireNonNull(node, "node");

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ExecutionContextBuilder dynamicParameters(Object... params) {
            this.dynamicParams = params;
            return this;
        }

        @Override
        public ExecutionContextBuilder timeZone(ZoneId zoneId) {
            this.zoneId = zoneId;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ExecutionContextBuilder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ExecutionContext<Object[]> build() {
            return new ExecutionContext<>(
                    new ExpressionFactoryImpl(
                            Commons.typeFactory(), 1024, CaffeineCacheFactory.INSTANCE
                    ),
                    Objects.requireNonNull(executor, "executor"),
                    new ExecutionId(queryId, 0),
                    Objects.requireNonNull(node, "node"),
                    node.name(),
                    node.id(),
                    description,
                    ArrayRowHandler.INSTANCE,
                    ArrayRowHandler.INSTANCE,
                    Commons.parametersMap(dynamicParams),
                    TxAttributes.fromTx(new NoOpTransaction(node.name(), false)),
                    zoneId,
                    -1,
                    clock,
                    null,
                    1L
            );
        }
    }

    static class ClusterBuilderImpl implements ClusterBuilder {
        private List<String> nodeNames;
        private final List<SystemView<?>> systemViews = new ArrayList<>();
        private final Map<String, Set<String>> nodeName2SystemView = new HashMap<>();

        private long planningTimeout = TimeUnit.SECONDS.toMillis(15);
        private Function<CatalogManager, CatalogManager> catalogManagerDecorator = Function.identity();
        private OperationKillHandler @Nullable [] killHandlers = null;

        private @Nullable DefaultDataProvider defaultDataProvider = null;
        private @Nullable DefaultAssignmentsProvider defaultAssignmentsProvider = null;

        /** {@inheritDoc} */
        @Override
        public ClusterBuilder nodes(String firstNodeName, String... otherNodeNames) {
            this.nodeNames = new ArrayList<>();

            nodeNames.add(firstNodeName);
            nodeNames.addAll(Arrays.asList(otherNodeNames));

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ClusterBuilder catalogManagerDecorator(Function<CatalogManager, CatalogManager> decorator) {
            this.catalogManagerDecorator = Objects.requireNonNull(decorator);

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ClusterBuilder operationKillHandlers(OperationKillHandler... handlers) {
            this.killHandlers = handlers;

            return this;
        }

        @Override
        public <T> ClusterBuilder addSystemView(SystemView<T> systemView) {
            systemViews.add(systemView);
            return this;
        }

        @Override
        public ClusterBuilder registerSystemView(String nodeName, String systemViewName) {
            nodeName2SystemView.computeIfAbsent(nodeName, key -> new HashSet<>()).add(systemViewName);

            return this;
        }

        @Override
        public ClusterBuilder defaultDataProvider(DefaultDataProvider defaultDataProvider) {
            this.defaultDataProvider = defaultDataProvider;

            return this;
        }

        @Override
        public ClusterBuilder defaultAssignmentsProvider(DefaultAssignmentsProvider defaultAssignmentsProvider) {
            this.defaultAssignmentsProvider = defaultAssignmentsProvider;

            return this;
        }

        @Override
        public ClusterBuilder planningTimeout(long value, TimeUnit timeUnit) {
            this.planningTimeout = timeUnit.toMillis(value);

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public TestCluster build() {
            var clusterName = "test_cluster";

            HybridClock clock = new HybridClockImpl();
            CatalogManager catalogManager = catalogManagerDecorator.apply(
                    CatalogTestUtils.createCatalogManagerWithTestUpdateLog(clusterName, clock)
            );

            var parserService = new ParserServiceImpl();

            ConcurrentMap<String, Long> tablesSize = new ConcurrentHashMap<>();
            var schemaManager = createSqlSchemaManager(catalogManager, tablesSize);

            ClockServiceImpl clockService = mock(ClockServiceImpl.class);

            when(clockService.currentLong()).thenReturn(new HybridTimestamp(1_000, 500).longValue());

            ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                    IgniteThreadFactory.create("test", "common-scheduled-executors", LOG)
            );

            Supplier<TableStatsStalenessConfiguration> statStalenessProperties = () -> new TableStatsStalenessConfiguration(
                    DEFAULT_STALE_ROWS_FRACTION, DEFAULT_MIN_STALE_ROWS_COUNT);

            @SuppressWarnings("unchecked")
            ConfigurationValue<Integer> configurationValue = mock(ConfigurationValue.class);
            when(configurationValue.value()).thenReturn(5);
            when(configurationValue.key()).thenReturn("staleRowsCheckIntervalSeconds");

            var prepareService = new PrepareServiceImpl(
                    clusterName,
                    0,
                    CaffeineCacheFactory.INSTANCE,
                    new DdlSqlToCommandConverter(storageProfiles -> completedFuture(null), filter -> completedFuture(null),
                            statStalenessProperties),
                    planningTimeout,
                    PLANNING_THREAD_COUNT,
                    PLAN_EXPIRATION_SECONDS,
                    new NoOpMetricManager(),
                    schemaManager,
                    clockService::currentLong,
                    scheduledExecutor,
                    mock(AbstractEventProducer.class),
                    configurationValue
            );

            Map<String, List<String>> systemViewsByNode = new HashMap<>();

            for (Entry<String, Set<String>> entry : nodeName2SystemView.entrySet()) {
                String nodeName = entry.getKey();
                for (String systemViewName : entry.getValue()) {
                    systemViewsByNode.computeIfAbsent(nodeName, (k) -> new ArrayList<>()).add(systemViewName);
                }
            }

            var clockWaiter = new ClockWaiter("test", clock, scheduledExecutor);
            var ddlHandler = new DdlCommandHandler(catalogManager, new TestClockService(clock, clockWaiter));

            Runnable initClosure = () -> {
                assertThat(clockWaiter.startAsync(new ComponentContext()), willCompleteSuccessfully());

                initAction(catalogManager);
            };

            RunnableX stopClosure = () -> IgniteUtils.shutdownAndAwaitTermination(scheduledExecutor, 10, TimeUnit.SECONDS);

            List<LogicalNode> logicalNodes = createLogicalNodes(nodeNames, systemViewsByNode);

            var logicalTopology = new LogicalTopologyImpl(
                    TestClusterStateStorage.initializedClusterStateStorage(),
                    ctx -> false
            );
            var clusterService = new ClusterServiceFactory(logicalTopology);

            logicalNodes.forEach(logicalTopology::putNode);

            ConcurrentMap<String, ScannableTable> dataProvidersByTableName = new ConcurrentHashMap<>();
            ConcurrentMap<String, UpdatableTable> updatableTablesByName = new ConcurrentHashMap<>();
            ConcurrentMap<String, AssignmentsProvider> assignmentsProviderByTableName = new ConcurrentHashMap<>();

            assignmentsProviderByTableName.put(
                    Blackhole.TABLE_NAME,
                    (partCount, ignored) -> IntStream.range(0, partCount)
                            .mapToObj(partNo -> nodeNames)
                            .collect(Collectors.toList())
            );

            DefaultDataProvider defaultDataProvider = this.defaultDataProvider;
            Map<String, TestNode> nodes = nodeNames.stream()
                    .map(name -> {
                        var systemViewManager = new SystemViewManagerImpl(name, catalogManager, mock(FailureProcessor.class));
                        var executionProvider = new TestExecutionDistributionProvider(
                                systemViewManager::owningNodes,
                                tableName -> resolveProvider(
                                        tableName,
                                        assignmentsProviderByTableName,
                                        defaultAssignmentsProvider != null ? defaultAssignmentsProvider::get : null
                                ),
                                false
                        );
                        var partitionPruner = new PartitionPrunerImpl();
                        var mappingService = new MappingServiceImpl(
                                name,
                                new TestClockService(clock, clockWaiter),
                                EmptyCacheFactory.INSTANCE,
                                0,
                                partitionPruner,
                                executionProvider,
                                Runnable::run
                        );

                        systemViewManager.register(() -> systemViews);

                        logicalTopology.addEventListener(mappingService);
                        logicalTopology.addEventListener(systemViewManager);

                        return new TestNode(
                                name,
                                catalogManager,
                                (TestClusterService) clusterService.forNode(name),
                                parserService,
                                prepareService,
                                schemaManager,
                                mappingService,
                                new TestExecutableTableRegistry(
                                        name0 -> resolveProvider(
                                                name0,
                                                dataProvidersByTableName,
                                                defaultDataProvider != null ? defaultDataProvider::get : null
                                        ),
                                        updatableTablesByName::get,
                                        schemaManager
                                ),
                                ddlHandler,
                                systemViewManager,
                                killHandlers
                        );
                    })
                    .collect(Collectors.toMap(TestNode::name, Function.identity()));

            logicalTopology.fireTopologyLeap();

            return new TestCluster(
                    tablesSize,
                    dataProvidersByTableName,
                    updatableTablesByName,
                    assignmentsProviderByTableName,
                    nodes,
                    catalogManager,
                    prepareService,
                    clockWaiter,
                    initClosure,
                    stopClosure
            );
        }
    }

    private static List<LogicalNode> createLogicalNodes(Collection<String> nodeNames, Map<String, List<String>> systemViewsByNode) {
        return nodeNames.stream()
                .map(name -> {
                    List<String> systemViewForNode = systemViewsByNode.getOrDefault(name, List.of());
                    NetworkAddress addr = NetworkAddress.from("127.0.0.1:10000");
                    LogicalNode logicalNode = new LogicalNode(randomUUID(), name, addr);

                    if (systemViewForNode.isEmpty()) {
                        return logicalNode;
                    } else {
                        String attrName = SystemViewManagerImpl.NODE_ATTRIBUTES_KEY;
                        String nodeNameSep = SystemViewManagerImpl.NODE_ATTRIBUTES_LIST_SEPARATOR;
                        String nodeNamesString = String.join(nodeNameSep, systemViewForNode);

                        return new LogicalNode(logicalNode, Map.of(), Map.of(attrName, nodeNamesString), List.of());
                    }
                })
                .collect(Collectors.toList());
    }

    private static void initAction(CatalogManager catalogManager) {
        // Every time an index is created add `start building `and `make available` commands
        // to make that index accessible to the SQL engine.
        Consumer<CreateIndexEventParameters> createIndexHandler = (params) -> {
            CatalogIndexDescriptor index = params.indexDescriptor();

            if (index.status() == CatalogIndexStatus.AVAILABLE) {
                return;
            }

            int indexId = index.id();

            CatalogCommand startBuildIndexCommand = StartBuildingIndexCommand.builder().indexId(indexId).build();
            CatalogCommand makeIndexAvailableCommand = MakeIndexAvailableCommand.builder().indexId(indexId).build();

            LOG.info("Index has been created. Sending commands to make index available. id: {}, name: {}, status: {}",
                    indexId, index.name(), index.status());

            catalogManager.execute(List.of(startBuildIndexCommand, makeIndexAvailableCommand))
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            LOG.error("Catalog command execution error", e);
                        }
                    });
        };
        catalogManager.listen(CatalogEvent.INDEX_CREATE, EventListener.fromConsumer(createIndexHandler));
    }

    private static SqlSchemaManagerImpl createSqlSchemaManager(CatalogManager catalogManager, ConcurrentMap<String, Long> tablesSize) {
        SqlStatisticManager sqlStatisticManager = tableId -> {
            CatalogTableDescriptor descriptor = catalogManager.activeCatalog(Long.MAX_VALUE).table(tableId);
            long fallbackSize = 10_000;

            if (descriptor == null) {
                return fallbackSize;
            }

            return tablesSize.getOrDefault(descriptor.name(), 10_000L);
        };

        return new SqlSchemaManagerImpl(
                catalogManager,
                sqlStatisticManager,
                CaffeineCacheFactory.INSTANCE,
                0
        );
    }

    private static class TableBuilderImpl implements TableBuilder {
        private final List<AbstractTableIndexBuilderImpl<?>> indexBuilders = new ArrayList<>();
        private final List<ColumnDescriptor> columns = new ArrayList<>();

        private String name;
        private IgniteDistribution distribution;
        private int size = 100_000;
        private Integer tableId;
        private int partitions = CatalogUtils.DEFAULT_PARTITION_COUNT;

        /** {@inheritDoc} */
        @Override
        public TableBuilder name(String name) {
            this.name = name;

            return this;
        }

        @Override
        public TableBuilder zoneName(String zoneName) {
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public SortedIndexBuilder sortedIndex() {
            return new SortedIndexBuilderImpl(this);
        }

        /** {@inheritDoc} */
        @Override
        public HashIndexBuilder hashIndex() {
            return new HashIndexBuilderImpl(this);
        }

        /** {@inheritDoc} */
        @Override
        public TableBuilder distribution(IgniteDistribution distribution) {
            this.distribution = distribution;

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public TableBuilder addColumn(String name, NativeType type, boolean nullable) {
            columns.add(new ColumnDescriptorImpl(
                    name, false, false, false, nullable, columns.size(), type, DefaultValueStrategy.DEFAULT_NULL, null
            ));

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public TableBuilder addColumn(String name, NativeType type) {
            return addColumn(name, type, true);
        }

        /** {@inheritDoc} */
        @Override
        public TableBuilder addColumn(String name, NativeType type, @Nullable Object defaultValue) {
            if (defaultValue == null) {
                return addColumn(name, type);
            } else {
                ColumnDescriptorImpl desc = new ColumnDescriptorImpl(
                        name, false, false, false, true, columns.size(), type, DefaultValueStrategy.DEFAULT_CONSTANT, () -> defaultValue
                );
                columns.add(desc);
            }

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public TableBuilder addKeyColumn(String name, NativeType type) {
            columns.add(new ColumnDescriptorImpl(
                    name, true, false, false, false, columns.size(), type, DefaultValueStrategy.DEFAULT_NULL, null
            ));

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public TableBuilder size(int size) {
            this.size = size;

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public TableBuilder tableId(int id) {
            this.tableId = id;

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public TableBuilder partitions(int num) {
            this.partitions = num;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public IgniteTable build() {
            if (distribution == null) {
                throw new IllegalArgumentException("Distribution is not specified");
            }

            if (name == null) {
                throw new IllegalArgumentException("Name is not specified");
            }

            if (columns.isEmpty()) {
                throw new IllegalArgumentException("Table must contain at least one column");
            }

            TableDescriptorImpl tableDescriptor = new TableDescriptorImpl(CollectionUtils.concat(columns,
                    List.of(new ColumnDescriptorImpl(
                                    Commons.PART_COL_NAME,
                                    false,
                                    true,
                                    true,
                                    false,
                                    columns.size(),
                                    NativeTypes.INT64,
                                    DefaultValueStrategy.DEFAULT_COMPUTED,
                                    () -> {
                                        throw new AssertionError("Partition virtual column is generated by a function");
                                    }),
                            new ColumnDescriptorImpl(
                                    Commons.PART_COL_NAME_LEGACY1,
                                    false,
                                    true,
                                    true,
                                    false,
                                    columns.size() + 1,
                                    NativeTypes.INT32,
                                    DefaultValueStrategy.DEFAULT_COMPUTED,
                                    () -> {
                                        throw new AssertionError("Partition virtual column is generated by a function");
                                    }),
                            new ColumnDescriptorImpl(
                                    Commons.PART_COL_NAME_LEGACY2,
                                    false,
                                    true,
                                    true,
                                    false,
                                    columns.size() + 2,
                                    NativeTypes.INT32,
                                    DefaultValueStrategy.DEFAULT_COMPUTED,
                                    () -> {
                                        throw new AssertionError("Partition virtual column is generated by a function");
                                    }
                            ))), distribution);

            Map<String, IgniteIndex> indexes = indexBuilders.stream()
                    .map(idx -> idx.build(tableDescriptor))
                    .collect(Collectors.toUnmodifiableMap(IgniteIndex::name, Function.identity()));

            return new IgniteTableImpl(
                    Objects.requireNonNull(name),
                    tableId != null ? tableId : TABLE_ID_GEN.incrementAndGet(),
                    1,
                    1L,
                    tableDescriptor,
                    findPrimaryKey(tableDescriptor, indexes.values()),
                    new TestStatistic(size),
                    indexes,
                    partitions,
                    ZONE_ID
            );
        }
    }

    private static ImmutableIntList findPrimaryKey(TableDescriptor descriptor, Collection<IgniteIndex> indexList) {
        IgniteIndex primaryKey = indexList.stream()
                .filter(IgniteIndex::primaryKey)
                .findFirst()
                .orElse(null);

        if (primaryKey != null) {
            return primaryKey.collation().getKeys();
        }

        List<Integer> list = new ArrayList<>();
        for (ColumnDescriptor column : descriptor) {
            if (column.key()) {
                list.add(column.logicalIndex());
            }
        }

        return ImmutableIntList.copyOf(list);
    }

    private static class SortedIndexBuilderImpl extends AbstractTableIndexBuilderImpl<SortedIndexBuilder>
            implements SortedIndexBuilder {
        private final TableBuilderImpl parent;

        private boolean primary;

        private SortedIndexBuilderImpl(TableBuilderImpl parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override
        SortedIndexBuilder self() {
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public TableBuilder end() {
            parent.indexBuilders.add(this);

            return parent;
        }

        /** {@inheritDoc} */
        @Override
        public SortedIndexBuilder primaryKey(boolean value) {
            this.primary = value;
            return self();
        }

        /** {@inheritDoc} */
        @Override
        public TestIndex build(TableDescriptor desc) {
            if (name == null) {
                throw new IllegalArgumentException("Name is not specified");
            }

            if (columns.isEmpty()) {
                throw new IllegalArgumentException("Index must contain at least one column");
            }

            if (collations.size() != columns.size()) {
                throw new IllegalArgumentException("Collation must be specified for each of columns.");
            }

            return TestIndex.createSorted(name, columns, collations, desc, primary);
        }
    }

    private static class HashIndexBuilderImpl extends AbstractTableIndexBuilderImpl<HashIndexBuilder> implements HashIndexBuilder {
        private final TableBuilderImpl parent;

        private boolean primary;

        private HashIndexBuilderImpl(TableBuilderImpl parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override
        HashIndexBuilder self() {
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public TableBuilder end() {
            parent.indexBuilders.add(this);

            return parent;
        }

        /** {@inheritDoc} */
        @Override
        public HashIndexBuilder primaryKey(boolean value) {
            this.primary = value;
            return self();
        }

        /** {@inheritDoc} */
        @Override
        public TestIndex build(TableDescriptor desc) {
            if (name == null) {
                throw new IllegalArgumentException("Name is not specified");
            }

            if (columns.isEmpty()) {
                throw new IllegalArgumentException("Index must contain at least one column");
            }

            assert collations == null : "Collation is not supported.";

            return TestIndex.createHash(name, columns, desc, primary);
        }
    }

    private abstract static class AbstractIndexBuilderImpl<ChildT> implements SortedIndexBuilderBase<ChildT>, HashIndexBuilderBase<ChildT> {
        String name;
        final List<String> columns = new ArrayList<>();
        List<Collation> collations;

        /** {@inheritDoc} */
        @Override
        public ChildT name(String name) {
            this.name = name;

            return self();
        }

        /** {@inheritDoc} */
        @Override
        public ChildT addColumn(String columnName) {
            columns.add(columnName);

            return self();
        }

        /** {@inheritDoc} */
        @Override
        public ChildT addColumn(String columnName, Collation collation) {
            if (collations == null) {
                collations = new ArrayList<>();
            }

            columns.add(columnName);
            collations.add(collation);

            return self();
        }

        abstract ChildT self();
    }

    private abstract static class AbstractTableIndexBuilderImpl<ChildT> extends AbstractIndexBuilderImpl<ChildT> {
        abstract TestIndex build(TableDescriptor desc);
    }

    /**
     * Base interface describing the complete set of table-related fields.
     *
     * <p>The sole purpose of this interface is to keep in sync both variants of table's builders.
     *
     * @param <ChildT> An actual type of builder that should be exposed to the user.
     * @see TableBuilder
     */
    private interface TableBuilderBase<ChildT> {
        /** Sets the name of the table. */
        ChildT name(String name);

        /** Sets the zone name of the table. */
        ChildT zoneName(String zoneName);

        /** Adds a key column to the table. */
        ChildT addKeyColumn(String name, NativeType type);

        /** Adds a column to the table. */
        ChildT addColumn(String name, NativeType type);

        /** Adds a column with given nullability to the table. */
        ChildT addColumn(String name, NativeType type, boolean nullable);

        /** Adds a column with the given default value to the table. */
        ChildT addColumn(String name, NativeType type, @Nullable Object defaultValue);
    }

    /**
     * Base interface describing the common set of index-related fields.
     *
     * <p>The sole purpose of this interface is to keep in sync both variants of index's builders.
     *
     * @param <ChildT> An actual type of builder that should be exposed to the user.
     * @see HashIndexBuilder
     * @see SortedIndexBuilder
     */
    private interface IndexBuilderBase<ChildT> {
        /** Sets the name of the index. */
        ChildT name(String name);
    }

    /**
     * Base interface describing the set of sorted-index related fields.
     *
     * @param <ChildT> An actual type of builder that should be exposed to the user.
     */
    private interface SortedIndexBuilderBase<ChildT> extends IndexBuilderBase<ChildT> {
        /** Adds a column with specified collation to the index. */
        ChildT addColumn(String columnName, Collation collation);
    }

    /**
     * Base interface describing the set of hash-index related fields.
     *
     * @param <ChildT> An actual type of builder that should be exposed to the user.
     */
    private interface HashIndexBuilderBase<ChildT> extends IndexBuilderBase<ChildT> {
        /** Adds a column to the index. */
        ChildT addColumn(String columnName);
    }

    /**
     * This interfaces provides a nested builder with ability to return on the previous layer.
     *
     * <p>For example:</p>
     * <pre>
     *     interface ChildBuilder implements NestedBuilder&lt;ParentBuilder&gt; {
     *         ChildBuilder nestedFoo();
     *     }
     *
     *     interface ParentBuilder {
     *         ParentBuilder foo();
     *         ParentBuilder bar();
     *         ChildBuilder child();
     *     }
     *
     *     Builders.parent()
     *         .foo()
     *         .child() // now we are dealing with the ChildBuilder
     *             .nestedFoo()
     *             .end() // and here we are returning back to the ParentBuilder
     *         .bar()
     *         .build()
     * </pre>
     */
    @FunctionalInterface
    interface NestedBuilder<ParentT> {
        /**
         * Notifies the builder's chain of the nested builder that we need to return back to the previous layer.
         *
         * @return An instance of the parent builder.
         */
        ParentT end();
    }

    private static class TestExecutableTableRegistry implements ExecutableTableRegistry {
        private final Function<String, ScannableTable> scannableTablesByName;
        private final Function<String, UpdatableTable> updatableTablesByName;
        private final SqlSchemaManager schemaManager;

        TestExecutableTableRegistry(
                Function<String, ScannableTable> scannableTablesByName,
                Function<String, UpdatableTable> updatableTablesByName,
                SqlSchemaManager schemaManager
        ) {
            this.scannableTablesByName = scannableTablesByName;
            this.updatableTablesByName = updatableTablesByName;
            this.schemaManager = schemaManager;
        }

        @Override
        public ExecutableTable getTable(int catalogVersion, int tableId) {
            IgniteTable table = schemaManager.table(catalogVersion, tableId);

            assert table != null;

            return new ExecutableTable() {
                @Override
                public ScannableTable scannableTable() {
                    ScannableTable scannableTable = scannableTablesByName.apply(table.name());

                    assert scannableTable != null;

                    return scannableTable;
                }

                @Override
                public UpdatableTable updatableTable() {
                    if (Blackhole.TABLE_NAME.equals(table.name())) {
                        return Blackhole.INSTANCE;
                    }

                    UpdatableTable updatableTable = updatableTablesByName.apply(table.name());

                    assert updatableTable != null;

                    return (UpdatableTable) Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {UpdatableTable.class},
                            (proxy, method, args) -> {
                                if ("descriptor".equals(method.getName())) {
                                    return table.descriptor();
                                }

                                return method.invoke(updatableTable, args); 
                            }
                    );
                }

                @Override
                public TableDescriptor tableDescriptor() {
                    return table.descriptor();
                }

                @Override
                public Supplier<PartitionCalculator> partitionCalculator() {
                    return table.partitionCalculator();
                }
            };
        }
    }

    private static Object[] project(Object[] row, int @Nullable [] requiredColumns) {
        if (requiredColumns == null) {
            return row;
        }

        Object[] newRow = new Object[requiredColumns.length];

        for (int i = 0; i < requiredColumns.length; i++) {
            newRow[i] = row[requiredColumns[i]];
        }

        return newRow;
    }

    /** Returns a builder for {@link ExecutionDistributionProvider}. */
    public static ExecutionDistributionProviderBuilder executionDistributionProviderBuilder() {
        return new ExecutionDistributionProviderBuilder();
    }

    /** A builder to create instances of {@link ExecutionDistributionProvider}. */
    public static final class ExecutionDistributionProviderBuilder {

        private final Map<String, List<List<String>>> owningNodesByTableName = new HashMap<>();

        private Function<String, List<String>> owningNodesBySystemViewName = (n) -> null;

        private boolean useTablePartitions;

        private ExecutionDistributionProviderBuilder() {

        }

        /** Adds tables to list of nodes mapping. */
        public ExecutionDistributionProviderBuilder addTables(Map<String, List<List<String>>> tables) {
            this.owningNodesByTableName.putAll(tables);
            return this;
        }

        /**
         * Sets a function that returns system views. Function accepts a view name and returns a list of nodes a system view is available
         * at.
         */
        public ExecutionDistributionProviderBuilder setSystemViews(Function<String, List<String>> systemViews) {
            this.owningNodesBySystemViewName = systemViews;
            return this;
        }

        /** Use table partitions to build mapping targets. Default is {@code false}. */
        public ExecutionDistributionProviderBuilder useTablePartitions(boolean value) {
            useTablePartitions = value;
            return this;
        }

        /** Creates an instance of {@link ExecutionDistributionProvider}. */
        public ExecutionDistributionProvider build() {
            Map<String, List<List<String>>> owningNodesByTableName = Map.copyOf(this.owningNodesByTableName);

            Function<String, AssignmentsProvider> sourceProviderFunction = tableName ->
                    (AssignmentsProvider) (partitionsCount, includeBackups) -> {
                        List<List<String>> assignments = owningNodesByTableName.get(tableName);

                        if (nullOrEmpty(assignments)) {
                            throw new AssertionError("Assignments are not configured for table " + tableName);
                        }

                        if (includeBackups) {
                            return assignments;
                        } else {
                            List<List<String>> primaryAssignments = new ArrayList<>();

                            for (List<String> assign : assignments) {
                                primaryAssignments.add(List.of(assign.get(0)));
                            }

                            return primaryAssignments;
                        }
                    };

            return new TestExecutionDistributionProvider(
                    owningNodesBySystemViewName,
                    sourceProviderFunction,
                    useTablePartitions
            );
        }
    }

    private static class TestExecutionDistributionProvider implements ExecutionDistributionProvider {
        final Function<String, List<String>> owningNodesBySystemViewName;

        final Function<String, AssignmentsProvider> owningNodesByTableName;

        final boolean useTablePartitions;

        private TestExecutionDistributionProvider(
                Function<String, List<String>> owningNodesBySystemViewName,
                Function<String, AssignmentsProvider> owningNodesByTableName,
                boolean useTablePartitions
        ) {
            this.owningNodesBySystemViewName = owningNodesBySystemViewName;
            this.owningNodesByTableName = owningNodesByTableName;
            this.useTablePartitions = useTablePartitions;
        }

        private static TokenizedAssignments partitionNodesToAssignment(List<String> nodes, long token) {
            return new TokenizedAssignmentsImpl(
                    nodes.stream().map(Assignment::forPeer).collect(Collectors.toSet()),
                    token
            );
        }

        @Override
        public CompletableFuture<List<TokenizedAssignments>> forTable(
                HybridTimestamp operationTime,
                IgniteTable table,
                boolean includeBackups
        ) {
            AssignmentsProvider provider = owningNodesByTableName.apply(table.name());

            if (provider == null) {
                return CompletableFuture.failedFuture(
                        new AssertionError("AssignmentsProvider is not configured for table " + table.name())
                );
            }
            List<List<String>> owningNodes = provider.get(table.partitions(), includeBackups);

            if (nullOrEmpty(owningNodes) || owningNodes.size() != table.partitions()) {
                throw new AssertionError("Configured AssignmentsProvider returns less assignment than expected "
                        + "[table=" + table.name() + ", expectedNumberOfPartitions=" + table.partitions()
                        + ", returnedAssignmentSize=" + (owningNodes == null ? "<null>" : owningNodes.size()) + "]");
            }

            List<TokenizedAssignments> assignments;

            if (useTablePartitions) {
                int p = table.partitions();

                assignments = IntStream.range(0, p).mapToObj(n -> {
                    List<String> nodes = owningNodes.get(n % owningNodes.size());
                    return partitionNodesToAssignment(nodes, p);
                }).collect(Collectors.toList());
            } else {
                assignments = owningNodes.stream()
                        .map(nodes -> partitionNodesToAssignment(nodes, 1))
                        .collect(Collectors.toList());
            }

            return completedFuture(assignments);
        }

        @Override
        public List<String> forSystemView(IgniteSystemView view) {
            List<String> nodes = owningNodesBySystemViewName.apply(view.name());

            if (nullOrEmpty(nodes)) {
                throw new SqlException(Sql.MAPPING_ERR, format("The view with name '{}' could not be found on"
                        + " any active nodes in the cluster", view));
            }

            return view.distribution() == IgniteDistributions.single() ? List.of(nodes.get(0)) : nodes;
        }
    }

    private abstract static class AbstractScannableTable implements ScannableTable {
        @Override
        public <RowT> Publisher<RowT> scan(
                ExecutionContext<RowT> ctx,
                PartitionWithConsistencyToken partWithConsistencyToken,
                RowFactory<RowT> rowFactory,
                int @Nullable [] requiredColumns
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <RowT> Publisher<RowT> indexRangeScan(
                ExecutionContext<RowT> ctx,
                PartitionWithConsistencyToken partWithConsistencyToken,
                RowFactory<RowT> rowFactory,
                int indexId,
                List<String> columns,
                @Nullable RangeCondition<RowT> cond,
                int @Nullable [] requiredColumns) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <RowT> Publisher<RowT> indexLookup(
                ExecutionContext<RowT> ctx,
                PartitionWithConsistencyToken partWithConsistencyToken,
                RowFactory<RowT> rowFactory,
                int indexId,
                List<String> columns,
                RowT key,
                int @Nullable [] requiredColumns
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <RowT> CompletableFuture<@Nullable RowT> primaryKeyLookup(
                ExecutionContext<RowT> ctx,
                InternalTransaction explicitTx,
                RowFactory<RowT> rowFactory,
                RowT key,
                int @Nullable [] requiredColumns
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Long> estimatedSize() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Data provider that will be used in case no other provider was specified explicitly via
     * {@link TestCluster#setDataProvider(String, ScannableTable)}.
     */
    @FunctionalInterface
    public interface DefaultDataProvider {
        ScannableTable get(String tableName);
    }

    /**
     * Assignments provider that will be used in case no other provider was specified explicitly via
     * {@link TestCluster#setAssignmentsProvider(String, AssignmentsProvider)}.
     */
    @FunctionalInterface
    public interface DefaultAssignmentsProvider {
        AssignmentsProvider get(String tableName);
    }

    /** Provider of assignments for a table. */
    @FunctionalInterface
    public interface AssignmentsProvider {
        /**
         * Returns the list of assignments.
         *
         * <p>Returned list must have the same number of elements as provided {@code partitionsCount}. If {@code includeBackups} is set to
         * {@code true}, then every sublist is allowed to have more than one element.
         *
         * @param partitionsCount Number of partitions in a table.
         * @param includeBackups Whether to include backup assignments or node.
         * @return List of assignments.
         */
        List<List<String>> get(int partitionsCount, boolean includeBackups);
    }

    private static <T> @Nullable T resolveProvider(
            String tableName,
            Map<String, T> providersByTableName,
            @Nullable Function<String, T> defaultProvider
    ) {
        T provider = providersByTableName.get(tableName);

        if (provider == null && defaultProvider != null) {
            return defaultProvider.apply(tableName);
        }

        return provider;
    }

    private static class Blackhole implements UpdatableTable {
        static final String TABLE_NAME = "BLACKHOLE";

        private static final TableDescriptor DESCRIPTOR = new TableDescriptorImpl(
                List.of(new ColumnDescriptorImpl("X", true, false, false, false, 0,
                        NativeTypes.INT32, DefaultValueStrategy.DEFAULT_NULL, null)), IgniteDistributions.single()
        );

        private static final UpdatableTable INSTANCE = new Blackhole();

        @Override
        public TableDescriptor descriptor() {
            return DESCRIPTOR;
        }

        @Override
        public <RowT> CompletableFuture<?> insertAll(ExecutionContext<RowT> ectx, List<RowT> rows, ColocationGroup colocationGroup) {
            return nullCompletedFuture();
        }

        @Override
        public <RowT> CompletableFuture<Void> insert(@Nullable InternalTransaction explicitTx, ExecutionContext<RowT> ectx, RowT row) {
            return nullCompletedFuture();
        }

        @Override
        public <RowT> CompletableFuture<?> upsertAll(ExecutionContext<RowT> ectx, List<RowT> rows, ColocationGroup colocationGroup) {
            return nullCompletedFuture();
        }

        @Override
        public <RowT> CompletableFuture<Boolean> delete(@Nullable InternalTransaction explicitTx, ExecutionContext<RowT> ectx, RowT key) {
            return completedFuture(false);
        }

        @Override
        public <RowT> CompletableFuture<?> deleteAll(ExecutionContext<RowT> ectx, List<RowT> rows, ColocationGroup colocationGroup) {
            return nullCompletedFuture();
        }
    }

    /**
     * Creates a cluster and runs a simple query to facilitate loading of necessary classes to prepare and execute sql queries.
     *
     * @throws Exception An exception if something goes wrong.
     */
    public static void warmupTestCluster() throws Exception {
        TestCluster cluster = cluster()
                .nodes("N1")
                .defaultDataProvider(tableName -> tableScan(DataProvider.fromCollection(List.of())))
                .defaultAssignmentsProvider(tableName -> (partitionsCount, includeBackups) -> IntStream.range(0, partitionsCount)
                        .mapToObj(i -> List.of("N1"))
                        .collect(Collectors.toList()))
                .build();

        cluster.start();

        try {
            TestNode node = cluster.node("N1");

            node.initSchema("CREATE TABLE t (id INT PRIMARY KEY, val INT)");

            await(node.executeQuery("SELECT * FROM t").requestNextAsync(1));
        } finally {
            cluster.stop();
        }
    }
}
