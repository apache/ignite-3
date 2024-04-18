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
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistry;
import org.apache.ignite.internal.sql.engine.exec.ExecutionDependencyResolver;
import org.apache.ignite.internal.sql.engine.exec.ExecutionDependencyResolverImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistry;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingService;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;

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
    private final MessageService messageService;

    private final List<LifecycleAware> services = new ArrayList<>();
    volatile boolean exceptionRaised;
    private final IgniteSpinBusyLock holdLock;
    private final HybridClock clock = new HybridClockImpl();
    private final ClockService clockService = new TestClockService(clock);

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
            ParserService parserService,
            PrepareService prepareService,
            SqlSchemaManager schemaManager,
            MappingService mappingService,
            ExecutableTableRegistry tableRegistry,
            DdlCommandHandler ddlCommandHandler,
            SystemViewManager systemViewManager
    ) {
        this.nodeName = nodeName;
        this.parserService = parserService;
        this.prepareService = prepareService;
        this.schemaManager = schemaManager;

        TopologyService topologyService = clusterService.topologyService();
        MessagingService messagingService = clusterService.messagingService();
        RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;

        MailboxRegistry mailboxRegistry = registerService(new MailboxRegistryImpl());

        FailureProcessor failureProcessor = new FailureProcessor(nodeName, (a, b) -> exceptionRaised = true);
        QueryTaskExecutorImpl queryExec = new QueryTaskExecutorImpl(nodeName, 4, failureProcessor);

        QueryTaskExecutor taskExecutor = registerService(queryExec);

        holdLock = new IgniteSpinBusyLock();

        messageService = registerService(new MessageServiceImpl(
                nodeName, messagingService, taskExecutor, holdLock, clockService
        ));
        ExchangeService exchangeService = registerService(new ExchangeServiceImpl(
                mailboxRegistry, messageService, clockService
        ));
        ExecutionDependencyResolver dependencyResolver = new ExecutionDependencyResolverImpl(
                tableRegistry, view -> () -> systemViewManager.scanView(view.name())
        );

        executionService = registerService(ExecutionServiceImpl.create(
                topologyService,
                messageService,
                schemaManager,
                ddlCommandHandler,
                taskExecutor,
                rowHandler,
                mailboxRegistry,
                exchangeService,
                mappingService,
                tableRegistry,
                dependencyResolver,
                clockService,
                5_000
        ));

        registerService(new IgniteComponentLifecycleAwareAdapter(systemViewManager));
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        services.forEach(LifecycleAware::start);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        holdLock.block();

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

    MessageService messageService() {
        return messageService;
    }

    IgniteSpinBusyLock holdLock() {
        return holdLock;
    }

    HybridClock clock() {
        return clock;
    }

    ClockService clockService() {
        return clockService;
    }

    /**
     * Executes given plan on a cluster this node belongs to
     * and returns an async cursor representing the result.
     *
     * @param plan A plan to execute.
     * @param transaction External transaction.
     * @return A cursor representing the result.
     */
    public AsyncCursor<InternalSqlRow> executePlan(QueryPlan plan, @Nullable InternalTransaction transaction) {
        return executionService.executePlan(transaction == null ? new NoOpTransaction(nodeName) : transaction, plan, createContext());
    }

    /**
     * Executes given plan on a cluster this node belongs to
     * and returns an async cursor representing the result.
     *
     * @param plan A plan to execute.
     * @return A cursor representing the result.
     */
    public AsyncCursor<InternalSqlRow> executePlan(QueryPlan plan) {
        return executePlan(plan, null);
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

    /**
     * Executes the given script.
     *
     * <p>This method splits given string by semicolon and execute every statement
     * one by one. Technically it may execute SELECT statements as well, but since
     * it returns nothing, it doesn't make any sense.
     *
     * @param script Script to execute.
     */
    public void initSchema(String script) {
        for (String statement : script.split(";")) {
            if (StringUtils.nullOrBlank(statement) || statement.trim().startsWith("--")) {
                continue;
            }

            ParsedResult parsedResult = parserService.parse(statement);
            BaseQueryContext ctx = createContext();

            QueryPlan plan = await(prepareService.prepareAsync(parsedResult, ctx));

            if (plan.type() != SqlQueryType.DDL && plan.type() != SqlQueryType.DML) {
                continue;
            }

            AsyncCursor<?> cursor = executionService.executePlan(new NoOpTransaction("tx"), plan, ctx);

            await(cursor.requestNextAsync(1));
        }
    }

    private BaseQueryContext createContext() {
        return BaseQueryContext.builder()
                .queryId(UUID.randomUUID())
                .cancel(new QueryCancel())
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(schemaManager.schema(Long.MAX_VALUE).getSubSchema(DEFAULT_SCHEMA_NAME))
                                .build()
                )
                .timeZoneId(SqlQueryProcessor.DEFAULT_TIME_ZONE_ID)
                .build();
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
    }

    private static class IgniteComponentLifecycleAwareAdapter implements LifecycleAware {

        final IgniteComponent component;

        private IgniteComponentLifecycleAwareAdapter(IgniteComponent component) {
            this.component = component;
        }

        @Override
        public void start() {
            component.startAsync();
        }

        @Override
        public void stop() throws Exception {
            component.stopAsync();
        }
    }
}
