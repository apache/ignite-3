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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTarget;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetProvider;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptorImpl;
import org.apache.ignite.internal.sql.engine.schema.DefaultValueStrategy;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptorImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * A collection of builders to create test objects.
 */
public class TestBuilders {

    /** Schema version. */
    public static final int SCHEMA_VERSION = -1;

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
         * When specified the given factory is used to create instances of
         * {@link ClusterTableBuilder#defaultDataProvider(DataProvider) default data providers} for tables that have no
         * {@link ClusterTableBuilder#defaultDataProvider(DataProvider) default data provider} set.
         *
         * <p>Note: when a table has default data provider this method has no effect.
         *
         * @return {@code this} for chaining.
         */
        ClusterBuilder defaultDataProviderFactory(DataProviderFactory dataProviderFactory);

        /**
         * Builds the cluster object.
         *
         * @return Created cluster object.
         */
        TestCluster build();
    }

    /**
     * A builder to create a test table object.
     *
     * @see TestTable
     */
    public interface TableBuilder extends TableBuilderBase<TableBuilder> {
        /** Returns a builder of the test sorted-index object. */
        public SortedIndexBuilder sortedIndex();

        /** Returns a builder of the test hash-index object. */
        public HashIndexBuilder hashIndex();

        /**
         * Builds a table.
         *
         * @return Created table object.
         */
        public TestTable build();
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
            DataSourceBuilder<ClusterTableBuilder>,
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
            DataSourceBuilder<ClusterSortedIndexBuilder>,
            NestedBuilder<ClusterTableBuilder> {
    }

    /**
     * A builder to create a test object, which represents hash index, as nested object of the cluster.
     *
     * @see TestIndex
     * @see TestCluster
     */
    public interface ClusterHashIndexBuilder extends HashIndexBuilderBase<ClusterHashIndexBuilder>,
            DataSourceBuilder<ClusterHashIndexBuilder>,
            NestedBuilder<ClusterTableBuilder> {
    }

    /**
     * A builder interface to enrich a builder object with data-source related fields.
     */
    public interface DataSourceBuilder<ChildT> {
        /**
         * Adds a default data provider, which will be used for those nodes for which no specific provider is specified.
         *
         * <p>Note: this method will force all nodes in the cluster to have a data provider for the given object.
         */
        ChildT defaultDataProvider(DataProvider<?> dataProvider);

        /** Adds a data provider for the given node to the data source object. */
        ChildT addDataProvider(String targetNode, DataProvider<?> dataProvider);
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
                    BaseQueryContext.builder().build(),
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
        private DataProviderFactory dataProviderFactory;
        private List<String> nodeNames;

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

        /** {@inheritDoc} */
        @Override
        public ClusterBuilder defaultDataProviderFactory(DataProviderFactory dataProviderFactory) {
            this.dataProviderFactory = dataProviderFactory;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public TestCluster build() {
            var clusterService = new ClusterServiceFactory(nodeNames);

            Map<String, Map<String, DataProvider<?>>> dataProvidersByTableName = new HashMap<>();
            for (ClusterTableBuilderImpl tableBuilder : tableBuilders) {
                validateDataSourceBuilder(tableBuilder);
                injectDefaultDataProvidersIfNeeded(tableBuilder);
                injectDataProvidersIfNeeded(tableBuilder);

                for (AbstractIndexBuilderImpl<?> indexBuilder : tableBuilder.indexBuilders) {
                    validateDataSourceBuilder(indexBuilder);
                    injectDataProvidersIfNeeded(indexBuilder);
                }

                dataProvidersByTableName.put(tableBuilder.name, tableBuilder.dataProviders);
            }

            Map<String, IgniteTable> tableByName = tableBuilders.stream()
                    .map(ClusterTableBuilderImpl::build)
                    .collect(Collectors.toMap(TestTable::name, Function.identity()));

            IgniteSchema schema = new IgniteSchema(DEFAULT_SCHEMA_NAME, SCHEMA_VERSION, tableByName.values());
            var schemaManager = new PredefinedSchemaManager(schema);
            var targetProvider = new ExecutionTargetProvider() {
                @Override
                public CompletableFuture<ExecutionTarget> forTable(ExecutionTargetFactory factory, IgniteTable table) {
                    Map<String, DataProvider<?>> dataProviders = dataProvidersByTableName.get(table.name());

                    if (nullOrEmpty(dataProviders)) {
                        throw new AssertionError("DataProvider is not configured for table " + table.name());
                    }

                    return CompletableFuture.completedFuture(factory.allOf(List.copyOf(dataProviders.keySet())));
                }

                @Override
                public CompletableFuture<ExecutionTarget> forSystemView(ExecutionTargetFactory factory, IgniteSystemView view) {
                    return CompletableFuture.failedFuture(new AssertionError("Not supported"));
                }
            };

            List<LogicalNode> logicalNodes = nodeNames.stream()
                    .map(name -> new LogicalNode(name, name, NetworkAddress.from("127.0.0.1:10000")))
                    .collect(Collectors.toList());

            Map<String, TestNode> nodes = nodeNames.stream()
                    .map(name -> {
                        var mappingService = new MappingServiceImpl(name, targetProvider);

                        mappingService.onTopologyLeap(new LogicalTopologySnapshot(1L, logicalNodes));

                        return new TestNode(
                                name, clusterService.forNode(name), schemaManager, mappingService
                        );
                    })
                    .collect(Collectors.toMap(TestNode::name, Function.identity()));

            return new TestCluster(nodes);
        }

        private void validateDataSourceBuilder(AbstractDataSourceBuilderImpl<?> tableBuilder) {
            Set<String> tableOwners = new HashSet<>(tableBuilder.dataProviders.keySet());

            tableOwners.removeAll(nodeNames);

            if (!tableOwners.isEmpty()) {
                throw new AssertionError(format("The table has a dataProvider that is outside the cluster "
                        + "[tableName={}, outsiders={}]", tableBuilder.name, tableOwners));
            }
        }

        private void injectDefaultDataProvidersIfNeeded(ClusterTableBuilderImpl tableBuilder) {
            if (tableBuilder.defaultDataProvider == null && dataProviderFactory != null) {
                tableBuilder.defaultDataProvider = dataProviderFactory.createDataProvider(tableBuilder.name, tableBuilder.columns);
            }
        }

        private void injectDataProvidersIfNeeded(AbstractDataSourceBuilderImpl<?> builder) {
            if (builder.defaultDataProvider == null) {
                return;
            }

            Set<String> nodesWithoutDataProvider = new HashSet<>(nodeNames);

            nodesWithoutDataProvider.removeAll(builder.dataProviders.keySet());

            for (String name : nodesWithoutDataProvider) {
                builder.addDataProvider(name, builder.defaultDataProvider);
            }
        }
    }

    private static class TableBuilderImpl extends AbstractTableBuilderImpl<TableBuilder> implements TableBuilder {
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
                    tableDescriptor,
                    Objects.requireNonNull(name),
                    size,
                    indexes
            );
        }

        /** {@inheritDoc} */
        @Override
        protected TableBuilder self() {
            return this;
        }
    }

    private static class ClusterTableBuilderImpl extends AbstractTableBuilderImpl<ClusterTableBuilder> implements ClusterTableBuilder {
        private final ClusterBuilderImpl parent;

        private ClusterTableBuilderImpl(ClusterBuilderImpl parent) {
            this.parent = parent;
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

        /** {@inheritDoc} */
        @Override
        protected ClusterTableBuilder self() {
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ClusterBuilder end() {
            parent.tableBuilders.add(this);

            return parent;
        }

        private TestTable build() {
            TableDescriptorImpl tableDescriptor = new TableDescriptorImpl(columns, distribution);

            List<IgniteIndex> indexes = indexBuilders.stream()
                    .map(idx -> idx.build(tableDescriptor))
                    .collect(Collectors.toList());

            return new TestTable(tableDescriptor, name, size, indexes, dataProviders);
        }
    }

    private static class SortedIndexBuilderImpl extends AbstractIndexBuilderImpl<SortedIndexBuilder>
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

            return TestIndex.createSorted(name, columns, collations, desc, dataProviders);
        }
    }

    private static class HashIndexBuilderImpl extends AbstractIndexBuilderImpl<HashIndexBuilder> implements HashIndexBuilder {
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

            return TestIndex.createHash(name, columns, desc, dataProviders);
        }
    }

    private static class ClusterSortedIndexBuilderImpl extends AbstractIndexBuilderImpl<ClusterSortedIndexBuilder>
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
        TestIndex build(TableDescriptor desc) {
            assert collations.size() == columns.size();

            return TestIndex.createSorted(name, columns, collations, desc, dataProviders);
        }
    }

    private static class ClusterHashIndexBuilderImpl extends AbstractIndexBuilderImpl<ClusterHashIndexBuilder>
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
        TestIndex build(TableDescriptor desc) {
            assert collations == null;

            return TestIndex.createHash(name, columns, desc, dataProviders);
        }
    }

    /**
     * A factory that creates {@link DataProvider data providers}.
     */
    @FunctionalInterface
    public interface DataProviderFactory {

        /**
         * Creates a {@link DataProvider} for the given table.
         *
         * @param tableName a table name.
         * @param columns a list of columns.
         * @return an instance of {@link DataProvider}.
         */
        DataProvider<Object[]> createDataProvider(String tableName, List<ColumnDescriptor> columns);
    }

    private abstract static class AbstractTableBuilderImpl<ChildT> extends AbstractDataSourceBuilderImpl<ChildT>
            implements TableBuilderBase<ChildT> {
        protected final List<ColumnDescriptor> columns = new ArrayList<>();
        protected final List<AbstractIndexBuilderImpl> indexBuilders = new ArrayList<>();

        protected IgniteDistribution distribution;
        protected int size = 100_000;

        /** {@inheritDoc} */
        @Override
        public ChildT name(String name) {
            this.name = name;

            return self();
        }

        /** {@inheritDoc} */
        @Override
        public ChildT distribution(IgniteDistribution distribution) {
            this.distribution = distribution;

            return self();
        }

        /** {@inheritDoc} */
        @Override
        public ChildT addKeyColumn(String name, NativeType type) {
            columns.add(new ColumnDescriptorImpl(
                    name, true, false, columns.size(), type, DefaultValueStrategy.DEFAULT_NULL, null
            ));

            return self();
        }

        /** {@inheritDoc} */
        @Override
        public ChildT addColumn(String name, NativeType type) {
            return addColumn(name, type, true);
        }

        /** {@inheritDoc} */
        @Override
        public ChildT addColumn(String name, NativeType type, boolean nullable) {
            columns.add(new ColumnDescriptorImpl(
                    name, false, nullable, columns.size(), type, DefaultValueStrategy.DEFAULT_NULL, null
            ));

            return self();
        }

        /** {@inheritDoc} */
        @Override
        public ChildT addColumn(String name, NativeType type, @Nullable Object defaultValue) {
            if (defaultValue == null) {
                return addColumn(name, type);
            } else {
                ColumnDescriptorImpl desc = new ColumnDescriptorImpl(
                        name, false, true, columns.size(), type, DefaultValueStrategy.DEFAULT_CONSTANT, () -> defaultValue
                );
                columns.add(desc);
            }
            return self();
        }

        /** {@inheritDoc} */
        @Override
        public ChildT size(int size) {
            this.size = size;

            return self();
        }
    }

    private abstract static class AbstractIndexBuilderImpl<ChildT> extends AbstractDataSourceBuilderImpl<ChildT>
            implements SortedIndexBuilderBase<ChildT>, HashIndexBuilderBase<ChildT> {
        protected final List<String> columns = new ArrayList<>();
        protected List<Collation> collations;

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

        abstract TestIndex build(TableDescriptor desc);
    }

    private abstract static class AbstractDataSourceBuilderImpl<ChildT> {

        protected String name;
        final Map<String, DataProvider<?>> dataProviders = new HashMap<>();
        DataProvider<?> defaultDataProvider = null;

        abstract ChildT self();

        public ChildT name(String name) {
            this.name = name;

            return self();
        }

        public ChildT defaultDataProvider(DataProvider<?> dataProvider) {
            this.defaultDataProvider = dataProvider;

            return self();
        }

        public ChildT addDataProvider(String targetNode, DataProvider<?> dataProvider) {
            this.dataProviders.put(targetNode, dataProvider);

            return self();
        }
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

        /** Sets the distribution of the table. */
        ChildT distribution(IgniteDistribution distribution);

        /** Adds a key column to the table. */
        ChildT addKeyColumn(String name, NativeType type);

        /** Adds a column to the table. */
        ChildT addColumn(String name, NativeType type);

        /** Adds a column with given nullability to the table. */
        ChildT addColumn(String name, NativeType type, boolean nullable);

        /** Adds a column with the given default value to the table. */
        ChildT addColumn(String name, NativeType type, @Nullable Object defaultValue);

        /** Sets the size of the table. */
        ChildT size(int size);
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
}
