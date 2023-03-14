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

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.schema.Table;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.sql.engine.exec.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.metadata.FragmentDescription;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptorImpl;
import org.apache.ignite.internal.sql.engine.schema.DefaultValueStrategy;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptorImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.network.ClusterNode;

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
     * A builder to create a test cluster object.
     *
     * @see TestCluster
     */
    public interface ClusterBuilder {
        /**
         * Sets desired names for the cluster nodes.
         *
         * @param firstNodeName A name of the first node. There is no difference in what node should be first. This parameter was introduced
         *     to force user to provide at least one node name.
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
         * {@link ClusterTableBuilder#defaultDataProvider(DataProvider) default data providers} for tables
         * that have no {@link ClusterTableBuilder#defaultDataProvider(DataProvider) default data provider} set.
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
        /**
         * Builds a table.
         *
         * @return Created table object.
         */
        public TestTable build();
    }

    /**
     * A builder to create a test table as nested object of the cluster.
     *
     * @see TestTable
     * @see TestCluster
     */
    public interface ClusterTableBuilder extends TableBuilderBase<ClusterTableBuilder>, NestedBuilder<ClusterBuilder> {
        /**
         * Adds a default data provider, which will be used for those nodes for which no specific provider is specified.
         *
         * <p>Note: this method will force all nodes in the cluster to have a data provider for the given table.
         */
        ClusterTableBuilder defaultDataProvider(DataProvider<?> dataProvider);
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
        private FragmentDescription description = new FragmentDescription(0, true, null, null, Long2ObjectMaps.emptyMap());

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

            for (ClusterTableBuilderImpl tableBuilder : tableBuilders) {
                validateTableBuilder(tableBuilder);
                injectDataProvidersIfNeeded(tableBuilder);
            }

            Map<String, Table> tableMap = tableBuilders.stream()
                    .map(ClusterTableBuilderImpl::build)
                    .collect(Collectors.toMap(TestTable::name, Function.identity()));

            var schemaManager = new PredefinedSchemaManager(new IgniteSchema("PUBLIC", tableMap, null));

            Map<String, TestNode> nodes = nodeNames.stream()
                    .map(name -> new TestNode(name, clusterService.forNode(name), schemaManager))
                    .collect(Collectors.toMap(TestNode::name, Function.identity()));

            return new TestCluster(nodes);
        }

        private void validateTableBuilder(ClusterTableBuilderImpl tableBuilder) {
            Set<String> tableOwners = new HashSet<>(tableBuilder.dataProviders.keySet());

            tableOwners.removeAll(nodeNames);

            if (!tableOwners.isEmpty()) {
                throw new AssertionError(format("The table has a dataProvider that is outside the cluster "
                        + "[tableName={}, outsiders={}]", tableBuilder.name, tableOwners));
            }
        }

        private void injectDataProvidersIfNeeded(ClusterTableBuilderImpl tableBuilder) {
            if (tableBuilder.defaultDataProvider == null) {
                if (dataProviderFactory != null) {
                    tableBuilder.defaultDataProvider = dataProviderFactory.createDataProvider(tableBuilder.name, tableBuilder.columns);
                } else {
                    return;
                }
            }

            Set<String> nodesWithoutDataProvider = new HashSet<>(nodeNames);

            nodesWithoutDataProvider.removeAll(tableBuilder.dataProviders.keySet());

            for (String name : nodesWithoutDataProvider) {
                tableBuilder.addDataProvider(name, tableBuilder.defaultDataProvider);
            }
        }
    }

    private static class TableBuilderImpl extends AbstractTableBuilderImpl<TableBuilder> implements TableBuilder {
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

            return new TestTable(
                    new TableDescriptorImpl(columns, distribution),
                    Objects.requireNonNull(name),
                    dataProviders,
                    size
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

        private DataProvider<?> defaultDataProvider = null;

        private ClusterTableBuilderImpl(ClusterBuilderImpl parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override
        protected ClusterTableBuilder self() {
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ClusterTableBuilder defaultDataProvider(DataProvider<?> dataProvider) {
            this.defaultDataProvider = dataProvider;

            return this;
        }

        /** {@inheritDoc} */
        @Override
        public ClusterBuilder end() {
            parent.tableBuilders.add(this);

            return parent;
        }

        private TestTable build() {
            return new TestTable(
                    new TableDescriptorImpl(columns, distribution), name, dataProviders, size
            );
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
         * @param tableName  a table name.
         * @param columns  a list of columns.
         *
         * @return  an instance of {@link DataProvider}.
         */
        DataProvider<Object[]> createDataProvider(String tableName, List<ColumnDescriptor> columns);
    }

    private abstract static class AbstractTableBuilderImpl<ChildT> implements TableBuilderBase<ChildT> {
        protected final List<ColumnDescriptor> columns = new ArrayList<>();
        protected final Map<String, DataProvider<?>> dataProviders = new HashMap<>();

        protected String name;
        protected IgniteDistribution distribution;
        protected int size = 100_000;

        protected abstract ChildT self();

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
        public ChildT addColumn(String name, NativeType type) {
            columns.add(new ColumnDescriptorImpl(
                    name, false, true, columns.size(), columns.size(), type, DefaultValueStrategy.DEFAULT_NULL, null
            ));

            return self();
        }

        /** {@inheritDoc} */
        @Override
        public ChildT addDataProvider(String targetNode, DataProvider<?> dataProvider) {
            this.dataProviders.put(targetNode, dataProvider);

            return self();
        }

        /** {@inheritDoc} */
        @Override
        public ChildT size(int size) {
            this.size = size;

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

        /** Adds a column to the table. */
        ChildT addColumn(String name, NativeType type);

        /** Adds a data provider for the given node to the table. */
        ChildT addDataProvider(String targetNode, DataProvider<?> dataProvider);

        /** Sets the size of the table. */
        ChildT size(int size);
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
         * Notifies the builder's chain of the nested builder that we need to return back to the
         * previous layer.
         *
         * @return An instance of the parent builder.
         */
        ParentT end();
    }
}
