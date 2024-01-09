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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImplTest.PLANNING_TIMEOUT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTable;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistry;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.NodeWithTerm;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithTerm;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.UpdatableTable;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTarget;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetProvider;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptorImpl;
import org.apache.ignite.internal.sql.engine.schema.DefaultValueStrategy;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptorImpl;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.type.BitmaskNativeType;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NumberNativeType;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.internal.util.TransformingIterator;
import org.apache.ignite.internal.util.subscription.TransformingPublisher;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * A collection of builders to create test objects.
 */
public class TestBuilders {
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
        return new ClusterServiceFactory(nodes);
    }

    /**
     * Factory method to create {@link ScannableTable table} instance from given data provider with
     * only implemented {@link ScannableTable#scan table scan}.
     */
    public static ScannableTable tableScan(DataProvider<Object[]> dataProvider) {
        return new ScannableTable() {
            @Override
            public <RowT> Publisher<RowT> scan(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm, RowFactory<RowT> rowFactory,
                    @Nullable BitSet requiredColumns) {

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

            @Override
            public <RowT> Publisher<RowT> indexRangeScan(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, @Nullable RangeCondition<RowT> cond,
                    @Nullable BitSet requiredColumns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, RowT key, @Nullable BitSet requiredColumns) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Factory method to create {@link ScannableTable table} instance from given data provider with
     * only implemented {@link ScannableTable#indexRangeScan index range scan}.
     */
    public static ScannableTable indexRangeScan(DataProvider<Object[]> dataProvider) {
        return new ScannableTable() {
            @Override
            public <RowT> Publisher<RowT> scan(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm, RowFactory<RowT> rowFactory,
                    @Nullable BitSet requiredColumns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RowT> Publisher<RowT> indexRangeScan(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, @Nullable RangeCondition<RowT> cond,
                    @Nullable BitSet requiredColumns) {
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

            @Override
            public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, RowT key, @Nullable BitSet requiredColumns) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Factory method to create {@link ScannableTable table} instance from given data provider with
     * only implemented {@link ScannableTable#indexLookup index lookup}.
     */
    public static ScannableTable indexLookup(DataProvider<Object[]> dataProvider) {
        return new ScannableTable() {
            @Override
            public <RowT> Publisher<RowT> scan(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm, RowFactory<RowT> rowFactory,
                    @Nullable BitSet requiredColumns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RowT> Publisher<RowT> indexRangeScan(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, @Nullable RangeCondition<RowT> cond,
                    @Nullable BitSet requiredColumns) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
                    RowFactory<RowT> rowFactory, int indexId, List<String> columns, RowT key, @Nullable BitSet requiredColumns) {
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
         * Creates a table builder to add to the cluster.
         *
         * @return An instance of table builder.
         */
        ClusterTableBuilder addTable();

        /**
         * Adds the given system view to the cluster.
         *
         * @param systemView System view.
         * @return {@code this} for chaining.
         * @param <T> System view data type.
         */
        <T> ClusterBuilder addSystemView(SystemView<T> systemView);

        /**
         * Builds the cluster object.
         *
         * @return Created cluster object.
         */
        TestCluster build();

        /**
         * Provides implementation of table with given name local per given node.
         *
         * @param nodeName Name of the node given instance of table will be assigned to.
         * @param tableName Name of the table given instance represents.
         * @param table Actual table that will be used for read operations during execution.
         * @return {@code this} for chaining.
         */
        ClusterBuilder dataProvider(String nodeName, String tableName, ScannableTable table);

        /**
         * Registers a previously added system view (see {@link #addSystemView(SystemView)}) on the specified node.
         *
         * @param nodeName Name of the node the view is going to be registered at.
         * @param systemViewName Name of previously registered system.
         * @return {@code this} for chaining.
         */
        ClusterBuilder registerSystemView(String nodeName, String systemViewName);
    }

    /**
     * A builder to create a test table object.
     *
     * @see TestTable
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

        /**
         * Builds a table.
         *
         * @return Created table object.
         */
        TestTable build();
    }

    /**
     * A builder to create a test object that representing sorted index.
     *
     * @see TestIndex
     */
    public interface SortedIndexBuilder extends SortedIndexBuilderBase<SortedIndexBuilder>, NestedBuilder<TableBuilder> {
    }

    /**
     * A builder to create a test object that representing hash index.
     *
     * @see TestIndex
     */
    public interface HashIndexBuilder extends HashIndexBuilderBase<HashIndexBuilder>, NestedBuilder<TableBuilder> {
    }

    /**
     * A builder to create a test table as nested object of the cluster.
     *
     * @see TestTable
     * @see TestCluster
     */
    public interface ClusterTableBuilder extends TableBuilderBase<ClusterTableBuilder>,
            NestedBuilder<ClusterBuilder> {

        /**
         * Creates a sorted-index builder to add to the cluster.
         *
         * @return An instance of sorted-index builder.
         */
        ClusterSortedIndexBuilder addSortedIndex();

        /**
         * Creates a hash-index builder to add to the cluster.
         *
         * @return An instance of hash builder.
         */
        ClusterHashIndexBuilder addHashIndex();
    }

    /**
     * A builder to create a test object, which represents sorted index, as nested object of the cluster.
     *
     * @see TestIndex
     * @see TestCluster
     */
    public interface ClusterSortedIndexBuilder extends SortedIndexBuilderBase<ClusterSortedIndexBuilder>,
            NestedBuilder<ClusterTableBuilder> {
    }

    /**
     * A builder to create a test object, which represents hash index, as nested object of the cluster.
     *
     * @see TestIndex
     * @see TestCluster
     */
    public interface ClusterHashIndexBuilder extends HashIndexBuilderBase<ClusterHashIndexBuilder>,
            NestedBuilder<ClusterTableBuilder> {
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
        ExecutionContextBuilder localNode(ClusterNode node);

        /**
         * Builds the context object.
         *
         * @return Created context object.
         */
        ExecutionContext<Object[]> build();
    }

    private static class ExecutionContextBuilderImpl implements ExecutionContextBuilder {
        private FragmentDescription description = new FragmentDescription(0, true, null, null, null);

        private UUID queryId = null;
        private QueryTaskExecutor executor = null;
        private ClusterNode node = null;

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
        public ExecutionContextBuilder localNode(ClusterNode node) {
            this.node = Objects.requireNonNull(node, "node");

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ExecutionContext<Object[]> build() {
            return new ExecutionContext<>(
                    Objects.requireNonNull(executor, "executor"),
                    queryId,
                    Objects.requireNonNull(node, "node"),
                    node.name(),
                    description,
                    ArrayRowHandler.INSTANCE,
                    Map.of(),
                    TxAttributes.fromTx(new NoOpTransaction(node.name()))
            );
        }
    }

    private static class ClusterBuilderImpl implements ClusterBuilder {
        private final List<ClusterTableBuilderImpl> tableBuilders = new ArrayList<>();
        private List<String> nodeNames;
        private final Map<String, Map<String, ScannableTable>> nodeName2tableName2table = new HashMap<>();
        private final List<SystemView<?>> systemViews = new ArrayList<>();
        private final Map<String, Set<String>> nodeName2SystemView = new HashMap<>();

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
        public ClusterTableBuilder addTable() {
            return new ClusterTableBuilderImpl(this);
        }

        @Override
        public <T> ClusterBuilder addSystemView(SystemView<T> systemView) {
            systemViews.add(systemView);
            return this;
        }

        @Override
        public ClusterBuilder dataProvider(String nodeName, String tableName, ScannableTable table) {
            nodeName2tableName2table.computeIfAbsent(nodeName, key -> new HashMap<>()).put(tableName, table);

            return this;
        }

        @Override
        public ClusterBuilder registerSystemView(String nodeName, String systemViewName) {
            nodeName2SystemView.computeIfAbsent(nodeName, key -> new HashSet<>()).add(systemViewName);

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public TestCluster build() {
            validateConfiguredDataProviders();

            var clusterService = new ClusterServiceFactory(nodeNames);

            var clusterName = "test_cluster";

            CatalogManagerImpl catalogManager = (CatalogManagerImpl)
                    CatalogTestUtils.createCatalogManagerWithTestUpdateLog(clusterName, new HybridClockImpl());

            var parserService = new ParserServiceImpl(0, EmptyCacheFactory.INSTANCE);
            var prepareService = new PrepareServiceImpl(clusterName, 0, CaffeineCacheFactory.INSTANCE,
                    new DdlSqlToCommandConverter(Map.of(), () -> "aipersist"), PLANNING_TIMEOUT, mock(MetricManager.class));

            Map<String, List<String>> owningNodesByTableName = new HashMap<>();
            for (Entry<String, Map<String, ScannableTable>> entry : nodeName2tableName2table.entrySet()) {
                for (String tableName : entry.getValue().keySet()) {
                    owningNodesByTableName.computeIfAbsent(tableName, key -> new ArrayList<>()).add(entry.getKey());
                }
            }

            Map<String, List<String>> systemViewsByNode = new HashMap<>();

            for (Entry<String, Set<String>> entry : nodeName2SystemView.entrySet()) {
                String nodeName = entry.getKey();
                for (String systemViewName : entry.getValue()) {
                    systemViewsByNode.computeIfAbsent(nodeName, (k) -> new ArrayList<>()).add(systemViewName);
                }
            }

            Runnable initClosure = () -> initAction(catalogManager);

            var ddlHandler = new DdlCommandHandler(catalogManager);
            var schemaManager = new SqlSchemaManagerImpl(catalogManager, CaffeineCacheFactory.INSTANCE, 0);

            List<LogicalNode> logicalNodes = nodeNames.stream()
                    .map(name -> {
                        List<String> systemViewForNode = systemViewsByNode.getOrDefault(name, List.of());
                        NetworkAddress addr = NetworkAddress.from("127.0.0.1:10000");
                        LogicalNode logicalNode = new LogicalNode(name, name, addr);

                        if (systemViewForNode.isEmpty()) {
                            return logicalNode;
                        } else {
                            String attrName = SystemViewManagerImpl.NODE_ATTRIBUTES_KEY;
                            String nodeNameSep = SystemViewManagerImpl.NODE_ATTRIBUTES_LIST_SEPARATOR;
                            String nodeNamesString = String.join(nodeNameSep, systemViewForNode);

                            return new LogicalNode(logicalNode, Map.of(), Map.of(attrName, nodeNamesString), Map.of());
                        }
                    })
                    .collect(Collectors.toList());

            Map<String, TestNode> nodes = nodeNames.stream()
                    .map(name -> {
                        var systemViewManager = new SystemViewManagerImpl(name, catalogManager);
                        var targetProvider = new TestNodeExecutionTargetProvider(systemViewManager::owningNodes, owningNodesByTableName);
                        var mappingService = new MappingServiceImpl(name, targetProvider, EmptyCacheFactory.INSTANCE, 0, Runnable::run);

                        systemViewManager.register(() -> systemViews);

                        LogicalTopologySnapshot newTopology = new LogicalTopologySnapshot(1L, logicalNodes);
                        mappingService.onTopologyLeap(newTopology);
                        systemViewManager.onTopologyLeap(newTopology);

                        return new TestNode(
                                name,
                                clusterService.forNode(name),
                                parserService,
                                prepareService,
                                schemaManager,
                                mappingService,
                                new TestExecutableTableRegistry(nodeName2tableName2table.get(name), schemaManager),
                                ddlHandler,
                                systemViewManager
                        );
                    })
                    .collect(Collectors.toMap(TestNode::name, Function.identity()));

            return new TestCluster(
                    nodes,
                    catalogManager,
                    prepareService,
                    initClosure
            );
        }

        private void validateConfiguredDataProviders() {
            Set<String> dataProvidersOwners = new HashSet<>(nodeName2tableName2table.keySet());

            dataProvidersOwners.removeAll(Set.copyOf(nodeNames));

            if (!dataProvidersOwners.isEmpty()) {
                Map<String, List<String>> problematicTables = new HashMap<>();

                for (String outsiderNode : dataProvidersOwners) {
                    for (String problematicTable : nodeName2tableName2table.get(outsiderNode).keySet()) {
                        problematicTables.computeIfAbsent(problematicTable, k -> new ArrayList<>()).add(outsiderNode);
                    }
                }

                String problematicTablesString = problematicTables.entrySet().stream()
                        .map(e -> e.getKey() + ": " + e.getValue())
                        .collect(Collectors.joining(", "));

                throw new AssertionError(format("The table has a dataProvider that is outside the cluster "
                        + "[{}]", problematicTablesString));
            }
        }

        private void initAction(CatalogManager catalogManager) {
            List<CatalogCommand> initialSchema = tableBuilders.stream()
                    .flatMap(builder -> builder.build().stream())
                    .collect(Collectors.toList());

            // Init schema
            await(catalogManager.execute(initialSchema));

            // Make indexes available
            List<CatalogCommand> makeIndexesAvailable = new ArrayList<>();

            Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

            for (ClusterTableBuilderImpl tableBuilder : tableBuilders) {
                String schemaName = tableBuilder.schemaName;
                CatalogSchemaDescriptor schema = catalog.schema(schemaName);
                assert schema != null : "SchemaDescriptor does not exist: " + schemaName;

                for (AbstractClusterTableIndexBuilderImpl<?> indexBuilder : tableBuilder.indexBuilders) {
                    String indexName = indexBuilder.name;
                    CatalogIndexDescriptor index = Arrays.stream(schema.indexes())
                            .filter(idx -> idx.name().equals(indexName))
                            .findAny()
                            .orElseThrow(() -> new AssertionError("IndexDescriptor does not exist: " + indexName));

                    CatalogCommand command = MakeIndexAvailableCommand.builder()
                            .indexId(index.id())
                            .build();
                    makeIndexesAvailable.add(command);
                }
            }

            await(catalogManager.execute(makeIndexesAvailable));
        }
    }

    private static class TableBuilderImpl implements TableBuilder {
        private final List<AbstractTableIndexBuilderImpl<?>> indexBuilders = new ArrayList<>();
        private final List<ColumnDescriptor> columns = new ArrayList<>();

        private String name;
        private IgniteDistribution distribution;
        private int size = 100_000;
        private Integer tableId;

        /** {@inheritDoc} */
        @Override
        public TableBuilder name(String name) {
            this.name = name;

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
                    name, false, nullable, columns.size(), type, DefaultValueStrategy.DEFAULT_NULL, null
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
                        name, false, true, columns.size(), type, DefaultValueStrategy.DEFAULT_CONSTANT, () -> defaultValue
                );
                columns.add(desc);
            }

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public TableBuilder addKeyColumn(String name, NativeType type) {
            columns.add(new ColumnDescriptorImpl(
                    name, true, false, columns.size(), type, DefaultValueStrategy.DEFAULT_NULL, null
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
        public TestTable build() {
            if (distribution == null) {
                throw new IllegalArgumentException("Distribution is not specified");
            }

            if (name == null) {
                throw new IllegalArgumentException("Name is not specified");
            }

            if (columns.isEmpty()) {
                throw new IllegalArgumentException("Table must contain at least one column");
            }

            TableDescriptorImpl tableDescriptor = new TableDescriptorImpl(columns, distribution);

            List<IgniteIndex> indexes = indexBuilders.stream()
                    .map(idx -> idx.build(tableDescriptor))
                    .collect(Collectors.toList());

            return new TestTable(
                    tableId != null ? tableId : TestTable.ID.incrementAndGet(),
                    tableDescriptor,
                    Objects.requireNonNull(name),
                    size,
                    indexes
            );
        }
    }

    private static class ClusterTableBuilderImpl implements ClusterTableBuilder {
        private final List<AbstractClusterTableIndexBuilderImpl<?>> indexBuilders = new ArrayList<>();

        private final List<ColumnParams> columns = new ArrayList<>();
        private final List<String> keyColumns = new ArrayList<>();

        private final ClusterBuilderImpl parent;

        private final String schemaName = CatalogManager.DEFAULT_SCHEMA_NAME;

        private String name;

        private ClusterTableBuilderImpl(ClusterBuilderImpl parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override
        public ClusterTableBuilder name(String name) {
            this.name = name;

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ClusterSortedIndexBuilder addSortedIndex() {
            return new ClusterSortedIndexBuilderImpl(this);
        }

        /** {@inheritDoc} */
        @Override
        public ClusterHashIndexBuilder addHashIndex() {
            return new ClusterHashIndexBuilderImpl(this);
        }

        @Override
        public ClusterTableBuilder addColumn(String name, NativeType type, boolean nullable) {
            columns.add(columnParams(name, type, nullable, null));

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ClusterTableBuilder addColumn(String name, NativeType type) {
            return addColumn(name, type, true);
        }

        @Override
        public ClusterTableBuilder addColumn(String name, NativeType type, @Nullable Object defaultValue) {
            columns.add(columnParams(name, type, true, defaultValue));

            return this;
        }

        @Override
        public ClusterTableBuilder addKeyColumn(String name, NativeType type) {
            keyColumns.add(name);

            return addColumn(name, type, false);
        }

        /** {@inheritDoc} */
        @Override
        public ClusterBuilder end() {
            parent.tableBuilders.add(this);

            return parent;
        }

        private List<CatalogCommand> build() {
            List<CatalogCommand> commands = new ArrayList<>(1 + indexBuilders.size());

            commands.add(
                    CreateTableCommand.builder()
                            .schemaName(schemaName)
                            .tableName(name)
                            .columns(columns)
                            .primaryKeyColumns(keyColumns)
                            .build()
            );

            for (AbstractClusterTableIndexBuilderImpl<?> builder : indexBuilders) {
                commands.add(builder.build(schemaName, name));
            }

            return commands;
        }
    }

    private static class SortedIndexBuilderImpl extends AbstractTableIndexBuilderImpl<SortedIndexBuilder>
            implements SortedIndexBuilder {
        private final TableBuilderImpl parent;

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

            return TestIndex.createSorted(name, columns, collations, desc);
        }
    }

    private static class HashIndexBuilderImpl extends AbstractTableIndexBuilderImpl<HashIndexBuilder> implements HashIndexBuilder {
        private final TableBuilderImpl parent;

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
        public TestIndex build(TableDescriptor desc) {
            if (name == null) {
                throw new IllegalArgumentException("Name is not specified");
            }

            if (columns.isEmpty()) {
                throw new IllegalArgumentException("Index must contain at least one column");
            }

            assert collations == null : "Collation is not supported.";

            return TestIndex.createHash(name, columns, desc);
        }
    }

    private static class ClusterSortedIndexBuilderImpl extends AbstractClusterTableIndexBuilderImpl<ClusterSortedIndexBuilder>
            implements ClusterSortedIndexBuilder {
        private final ClusterTableBuilderImpl parent;

        ClusterSortedIndexBuilderImpl(ClusterTableBuilderImpl parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override
        ClusterSortedIndexBuilder self() {
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ClusterTableBuilder end() {
            parent.indexBuilders.add(this);

            return parent;
        }

        @Override
        CatalogCommand build(String schemaName, String tableName) {
            assert collations.size() == columns.size();

            List<CatalogColumnCollation> catalogCollations = collations.stream()
                    .map(c -> CatalogColumnCollation.get(c.asc, c.nullsFirst))
                    .collect(Collectors.toList());

            return CreateSortedIndexCommand.builder()
                    .schemaName(schemaName)
                    .tableName(tableName)
                    .indexName(name)
                    .columns(columns)
                    .collations(catalogCollations)
                    .build();
        }
    }

    private static class ClusterHashIndexBuilderImpl extends AbstractClusterTableIndexBuilderImpl<ClusterHashIndexBuilder>
            implements ClusterHashIndexBuilder {
        private final ClusterTableBuilderImpl parent;

        ClusterHashIndexBuilderImpl(ClusterTableBuilderImpl parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override
        ClusterHashIndexBuilder self() {
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ClusterTableBuilder end() {
            parent.indexBuilders.add(this);

            return parent;
        }

        @Override
        CatalogCommand build(String schemaName, String tableName) {
            return CreateHashIndexCommand.builder()
                    .schemaName(schemaName)
                    .tableName(tableName)
                    .indexName(name)
                    .columns(columns)
                    .build();
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

    private abstract static class AbstractClusterTableIndexBuilderImpl<ChildT> extends AbstractIndexBuilderImpl<ChildT> {
        abstract CatalogCommand build(String schemaName, String tableName);
    }

    /**
     * Base interface describing the complete set of table-related fields.
     *
     * <p>The sole purpose of this interface is to keep in sync both variants of table's builders.
     *
     * @param <ChildT> An actual type of builder that should be exposed to the user.
     * @see ClusterTableBuilder
     * @see TableBuilder
     */
    private interface TableBuilderBase<ChildT> {
        /** Sets the name of the table. */
        ChildT name(String name);

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
     * @see ClusterHashIndexBuilder
     * @see ClusterSortedIndexBuilder
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
    private interface NestedBuilder<ParentT> {
        /**
         * Notifies the builder's chain of the nested builder that we need to return back to the previous layer.
         *
         * @return An instance of the parent builder.
         */
        ParentT end();
    }

    private static class TestExecutableTableRegistry implements ExecutableTableRegistry {
        private final Map<String, ScannableTable> tablesByName;
        private final SqlSchemaManager schemaManager;

        TestExecutableTableRegistry(Map<String, ScannableTable> tablesByName, SqlSchemaManager schemaManager) {
            this.tablesByName = tablesByName;
            this.schemaManager = schemaManager;
        }

        @Override
        public CompletableFuture<ExecutableTable> getTable(int schemaVersion, int tableId) {
            IgniteTable table = schemaManager.table(schemaVersion, tableId);

            assert table != null;

            return CompletableFuture.completedFuture(new ExecutableTable() {
                @Override
                public ScannableTable scannableTable() {
                    ScannableTable scannableTable = tablesByName.get(table.name());

                    assert scannableTable != null;

                    return scannableTable;
                }

                @Override
                public UpdatableTable updatableTable() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public TableDescriptor tableDescriptor() {
                    return table.descriptor();
                }
            });
        }
    }

    private static ColumnParams columnParams(String name, NativeType type, boolean nullable, @Nullable Object defaultValue) {
        NativeTypeSpec typeSpec = type.spec();

        Builder builder = ColumnParams.builder()
                .name(name)
                .type(typeSpec.asColumnType())
                .nullable(nullable)
                .defaultValue(DefaultValue.constant(defaultValue));

        switch (typeSpec) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case UUID:
            case BOOLEAN:
                break;
            case NUMBER:
                assert type instanceof NumberNativeType : type.getClass().getCanonicalName();

                builder.precision(((NumberNativeType) type).precision());
                break;
            case DECIMAL:
                assert type instanceof DecimalNativeType : type.getClass().getCanonicalName();

                builder.precision(((DecimalNativeType) type).precision());
                builder.scale(((DecimalNativeType) type).scale());
                break;
            case STRING:
            case BYTES:
                assert type instanceof VarlenNativeType : type.getClass().getCanonicalName();

                builder.length(((VarlenNativeType) type).length());
                break;
            case BITMASK:
                assert type instanceof BitmaskNativeType : type.getClass().getCanonicalName();

                builder.length(((BitmaskNativeType) type).bits());
                break;
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                assert type instanceof TemporalNativeType : type.getClass().getCanonicalName();

                builder.precision(((TemporalNativeType) type).precision());
                break;
            default:
                throw new IllegalArgumentException("Unsupported native type: " + typeSpec);
        }

        return builder.build();
    }

    private static Object[] project(Object[] row, @Nullable BitSet requiredElements) {
        if (requiredElements == null) {
            return row;
        }

        Object[] newRow = new Object[requiredElements.cardinality()];

        int idx = 0;
        for (int i = requiredElements.nextSetBit(0); i != -1; i = requiredElements.nextSetBit(i + 1)) {
            newRow[idx++] = row[i];
        }

        return newRow;
    }

    /** Returns a builder for {@link ExecutionTargetProvider}. */
    public static ExecutionTargetProviderBuilder executionTargetProviderBuilder() {
        return new ExecutionTargetProviderBuilder();
    }

    /** A builder to create instances of {@link ExecutionTargetProvider}. */
    public static final class ExecutionTargetProviderBuilder {

        private final Map<String, List<String>> owningNodesByTableName = new HashMap<>();

        private Function<String, List<String>> owningNodesBySystemViewName = (n) -> null;

        private ExecutionTargetProviderBuilder() {

        }

        /** Adds tables to list of nodes mapping. */
        public ExecutionTargetProviderBuilder addTables(Map<String, List<String>> tables) {
            this.owningNodesByTableName.putAll(tables);
            return this;
        }

        /**
         * Sets a function that returns system views. Function accepts a view name and returns a list of nodes
         * a system view is available at.
         */
        public ExecutionTargetProviderBuilder setSystemViews(Function<String, List<String>> systemViews) {
            this.owningNodesBySystemViewName = systemViews;
            return this;
        }

        /** Creates an instance of {@link ExecutionTargetProvider}. */
        public ExecutionTargetProvider build() {
            return new TestNodeExecutionTargetProvider(owningNodesBySystemViewName, Map.copyOf(owningNodesByTableName));
        }
    }

    private static class TestNodeExecutionTargetProvider implements ExecutionTargetProvider {

        final Function<String, List<String>> owningNodesBySystemViewName;

        final Map<String, List<String>> owningNodesByTableName;

        private TestNodeExecutionTargetProvider(
                Function<String, List<String>> owningNodesBySystemViewName,
                Map<String, List<String>> owningNodesByTableName) {

            this.owningNodesBySystemViewName = owningNodesBySystemViewName;
            this.owningNodesByTableName = Map.copyOf(owningNodesByTableName);
        }

        @Override
        public CompletableFuture<ExecutionTarget> forTable(ExecutionTargetFactory factory, IgniteTable table) {
            List<String> owningNodes = owningNodesByTableName.get(table.name());

            if (nullOrEmpty(owningNodes)) {
                throw new AssertionError("DataProvider is not configured for table " + table.name());
            }

            ExecutionTarget target = factory.partitioned(owningNodes.stream()
                    .map(name -> new NodeWithTerm(name, 1))
                    .collect(Collectors.toList()));

            return CompletableFuture.completedFuture(target);
        }

        @Override
        public CompletableFuture<ExecutionTarget> forSystemView(ExecutionTargetFactory factory, IgniteSystemView view) {
            List<String> nodes = owningNodesBySystemViewName.apply(view.name());

            if (nullOrEmpty(nodes)) {
                return CompletableFuture.failedFuture(
                        new SqlException(Sql.MAPPING_ERR, format("The view with name '{}' could not be found on"
                                + " any active nodes in the cluster", view.name()))
                );
            }

            return CompletableFuture.completedFuture(
                    view.distribution() == IgniteDistributions.single()
                            ? factory.oneOf(nodes)
                            : factory.allOf(nodes)
            );
        }
    }
}
