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
import static org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImplTest.PLANNING_TIMEOUT;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionDependencyResolver;
import org.apache.ignite.internal.sql.engine.exec.ExecutionDependencyResolverImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.LogicalRelImplementor;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistry;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.NoOpExecutableTableRegistry;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingService;
import org.apache.ignite.internal.sql.engine.exec.rel.Node;
import org.apache.ignite.internal.sql.engine.exec.rel.ScanNode;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.apache.ignite.internal.sql.engine.util.HashFunctionFactoryImpl;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyService;

/**
 * An object representing a node in test cluster.
 *
 * <p>Provides convenient access to the methods for optimization and execution of the queries.
 */
public class TestNode implements LifecycleAware {
    private final String nodeName;
    private final SqlSchemaManager schemaManager;
    private final PrepareService prepareService;
    private final ExecutionService executionService;
    private final ParserService parserService;

    private final List<LifecycleAware> services = new ArrayList<>();

    /**
     * Constructs the object.
     *
     * @param nodeName A name of the node to create.
     * @param clusterService A cluster service.
     * @param schemaManager A schema manager to use for query planning and execution.
     */
    TestNode(
            String nodeName,
            ClusterService clusterService,
            SqlSchemaManager schemaManager,
            MappingService mappingService
    ) {
        this.nodeName = nodeName;
        var ps = new PrepareServiceImpl(nodeName, 0, mock(DdlSqlToCommandConverter.class), PLANNING_TIMEOUT, mock(MetricManager.class));
        this.prepareService = registerService(ps);
        this.schemaManager = schemaManager;

        TopologyService topologyService = clusterService.topologyService();
        MessagingService messagingService = clusterService.messagingService();
        RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;

        MailboxRegistry mailboxRegistry = registerService(new MailboxRegistryImpl());
        QueryTaskExecutor taskExecutor = registerService(new QueryTaskExecutorImpl(nodeName));

        MessageService messageService = registerService(new MessageServiceImpl(
                topologyService.localMember().name(), messagingService, taskExecutor, new IgniteSpinBusyLock()
        ));
        ExchangeService exchangeService = registerService(new ExchangeServiceImpl(
                mailboxRegistry, messageService
        ));
        NoOpExecutableTableRegistry executableTableRegistry = new NoOpExecutableTableRegistry();
        ExecutionDependencyResolver dependencyResolver = new ExecutionDependencyResolverImpl(executableTableRegistry, null);

        executionService = registerService(new ExecutionServiceImpl<>(
                messageService,
                topologyService,
                mappingService,
                schemaManager,
                mock(DdlCommandHandler.class),
                taskExecutor,
                rowHandler,
                dependencyResolver,
                (ctx, deps) -> new LogicalRelImplementor<Object[]>(
                        ctx,
                        new HashFunctionFactoryImpl<>(rowHandler),
                        mailboxRegistry,
                        exchangeService,
                        deps
                ) {
                    @Override
                    public Node<Object[]> visit(IgniteTableScan rel) {
                        DataProvider<Object[]> dataProvider = rel.getTable().unwrap(TestTable.class).dataProvider(ctx.localNode().name());

                        return new ScanNode<>(ctx, dataProvider);
                    }

                    @Override
                    public Node<Object[]> visit(IgniteIndexScan rel) {
                        TestTable tbl = rel.getTable().unwrap(TestTable.class);
                        TestIndex idx = (TestIndex) tbl.indexes().get(rel.indexName());

                        DataProvider<Object[]> dataProvider = idx.dataProvider(ctx.localNode().name());

                        return new ScanNode<>(ctx, dataProvider);
                    }
                }
        ));

        parserService = new ParserServiceImpl(0, EmptyCacheFactory.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        services.forEach(LifecycleAware::start);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        List<AutoCloseable> closeables = services.stream()
                .map(service -> ((AutoCloseable) service::stop))
                .collect(Collectors.toList());

        Collections.reverse(closeables);
        IgniteUtils.closeAll(closeables);
    }

    /** Returns the name of the current node. */
    public String name() {
        return nodeName;
    }

    /**
     * Executes given plan on a cluster this node belongs to
     * and returns an async cursor representing the result.
     *
     * @param plan A plan to execute.
     * @return A cursor representing the result.
     */
    public AsyncCursor<List<Object>> executePlan(QueryPlan plan) {
        return executionService.executePlan(new NoOpTransaction(nodeName), plan, createContext());
    }

    /**
     * Prepares (aka parses, validates, and optimizes) the given query string
     * and returns the plan to execute.
     *
     * @param query A query string to prepare.
     * @return A plan to execute.
     */
    public QueryPlan prepare(String query) {
        ParsedResult parsedResult = parserService.parse(query);
        BaseQueryContext ctx = createContext();

        assertEquals(ctx.parameters().length, parsedResult.dynamicParamsCount(), "Invalid number of dynamic parameters");

        return await(prepareService.prepareAsync(parsedResult, ctx));
    }

    /**
     * Prepares (validates, and optimizes) the given query AST
     * and returns the plan to execute.
     *
     * @param parsedResult Parsed AST of a query to prepare.
     * @return A plan to execute.
     */
    public QueryPlan prepare(ParsedResult parsedResult) {
        return await(prepareService.prepareAsync(parsedResult, createContext()));
    }

    private BaseQueryContext createContext() {
        return BaseQueryContext.builder()
                .cancel(new QueryCancel())
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(schemaManager.schema(DEFAULT_SCHEMA_NAME, Long.MAX_VALUE))
                                .build()
                )
                .build();
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
    }
}
