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

import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.exec.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.LogicalRelImplementor;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistry;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.rel.Node;
import org.apache.ignite.internal.sql.engine.exec.rel.ScanNode;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.metadata.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.HashFunctionFactoryImpl;
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
    private final SchemaPlus schema;
    private final PrepareService prepareService;
    private final ExecutionService executionService;

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
            SqlSchemaManager schemaManager
    ) {
        this.nodeName = nodeName;
        this.prepareService = registerService(new PrepareServiceImpl(nodeName, 0, mock(DdlSqlToCommandConverter.class)));
        this.schema = schemaManager.schema("PUBLIC");

        TopologyService topologyService = clusterService.topologyService();
        MessagingService messagingService = clusterService.messagingService();
        RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;

        MailboxRegistry mailboxRegistry = registerService(new MailboxRegistryImpl());
        QueryTaskExecutor taskExecutor = registerService(new QueryTaskExecutorImpl(nodeName));

        MessageService messageService = registerService(new MessageServiceImpl(
                topologyService, messagingService, taskExecutor, new IgniteSpinBusyLock()
        ));
        ExchangeService exchangeService = registerService(new ExchangeServiceImpl(
                mailboxRegistry, messageService
        ));

        executionService = registerService(new ExecutionServiceImpl<>(
                topologyService.localMember(),
                messageService,
                topologyService,
                new MappingServiceImpl(topologyService),
                schemaManager,
                mock(DdlCommandHandler.class),
                taskExecutor,
                rowHandler,
                exchangeService,
                ctx -> new LogicalRelImplementor<Object[]>(
                        ctx,
                        new HashFunctionFactoryImpl<>(schemaManager, rowHandler),
                        mailboxRegistry,
                        exchangeService
                ) {
                    @Override
                    public Node<Object[]> visit(IgniteTableScan rel) {
                        DataProvider<Object[]> dataProvider = rel.getTable().unwrap(TestTable.class).dataProvider(ctx.localNode().name());

                        return new ScanNode<>(ctx, dataProvider);
                    }
                }
        ));
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
        return executionService.executePlan(plan, createContext());
    }

    /**
     * Prepares (aka parses, validates, and optimizes) the given query string
     * and returns the plan to execute.
     *
     * @param query A query string to prepare.
     * @return A plan to execute.
     */
    public QueryPlan prepare(String query) {
        SqlNodeList nodes = Commons.parse(query, FRAMEWORK_CONFIG.getParserConfig());

        assertThat(nodes, hasSize(1));

        return prepareService.prepareAsync(nodes.get(0), createContext()).join();
    }

    private BaseQueryContext createContext() {
        return BaseQueryContext.builder()
                .cancel(new QueryCancel())
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(schema)
                                .build()
                )
                .build();
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
    }
}
