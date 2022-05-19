/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.sql.engine.exec.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *  SqlQueryProcessor.
 *  TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class SqlQueryProcessor implements QueryProcessor {
    private static final IgniteLogger LOG = IgniteLogger.forClass(SqlQueryProcessor.class);

    /** Size of the cache for query plans. */
    public static final int PLAN_CACHE_SIZE = 1024;

    private final ClusterService clusterSrvc;

    private final TableManager tableManager;

    private final Consumer<Function<Long, CompletableFuture<?>>> registry;

    private final DataStorageManager dataStorageManager;

    private final Supplier<Map<String, Map<String, Class<?>>>> dataStorageFieldsSupplier;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Event listeners to close. */
    private final List<Pair<TableEvent, EventListener<TableEventParameters>>> evtLsnrs = new ArrayList<>();

    private final List<LifecycleAware> services = new ArrayList<>();

    private volatile QueryTaskExecutor taskExecutor;

    private volatile ExecutionService executionSrvc;

    private volatile PrepareService prepareSvc;

    private volatile SqlSchemaManager schemaManager;

    /** Constructor. */
    public SqlQueryProcessor(
            Consumer<Function<Long, CompletableFuture<?>>> registry,
            ClusterService clusterSrvc,
            TableManager tableManager,
            DataStorageManager dataStorageManager,
            Supplier<Map<String, Map<String, Class<?>>>> dataStorageFieldsSupplier
    ) {
        this.registry = registry;
        this.clusterSrvc = clusterSrvc;
        this.tableManager = tableManager;
        this.dataStorageManager = dataStorageManager;
        this.dataStorageFieldsSupplier = dataStorageFieldsSupplier;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void start() {
        var nodeName = clusterSrvc.topologyService().localMember().name();

        taskExecutor = registerService(new QueryTaskExecutorImpl(nodeName));
        var mailboxRegistry = registerService(new MailboxRegistryImpl());

        var prepareSvc = registerService(PrepareServiceImpl.create(
                nodeName,
                PLAN_CACHE_SIZE,
                dataStorageManager,
                dataStorageFieldsSupplier.get()
        ));

        var msgSrvc = registerService(new MessageServiceImpl(
                clusterSrvc.topologyService(),
                clusterSrvc.messagingService(),
                taskExecutor
        ));

        var exchangeService = registerService(new ExchangeServiceImpl(
                nodeName,
                taskExecutor,
                mailboxRegistry,
                msgSrvc
        ));

        SqlSchemaManagerImpl schemaManager = new SqlSchemaManagerImpl(tableManager, registry);

        schemaManager.registerListener(prepareSvc);

        this.prepareSvc = prepareSvc;

        var executionSrvc = registerService(ExecutionServiceImpl.create(
                clusterSrvc.topologyService(),
                msgSrvc,
                schemaManager,
                tableManager,
                taskExecutor,
                ArrayRowHandler.INSTANCE,
                mailboxRegistry,
                exchangeService,
                dataStorageManager
        ));

        clusterSrvc.topologyService().addEventHandler(executionSrvc);
        clusterSrvc.topologyService().addEventHandler(mailboxRegistry);

        this.executionSrvc = executionSrvc;

        registerTableListener(TableEvent.CREATE, new TableCreatedListener(schemaManager));
        registerTableListener(TableEvent.ALTER, new TableUpdatedListener(schemaManager));
        registerTableListener(TableEvent.DROP, new TableDroppedListener(schemaManager));

        this.schemaManager = schemaManager;

        services.forEach(LifecycleAware::start);
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
    }

    private void registerTableListener(TableEvent evt, AbstractTableEventListener lsnr) {
        evtLsnrs.add(Pair.of(evt, lsnr));

        tableManager.listen(evt, lsnr);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void stop() throws Exception {
        busyLock.block();

        List<LifecycleAware> services = new ArrayList<>(this.services);

        this.services.clear();

        Collections.reverse(services);

        Stream<AutoCloseable> closableComponents = services.stream().map(s -> s::stop);

        Stream<AutoCloseable> closableListeners = evtLsnrs.stream()
                .map((p) -> () -> tableManager.removeListener(p.left, p.right));

        IgniteUtils.closeAll(Stream.concat(closableComponents, closableListeners).collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    @Override
    public List<CompletableFuture<AsyncSqlCursor<List<Object>>>> queryAsync(String schemaName, String qry, Object... params) {
        return queryAsync(QueryContext.of(), schemaName, qry, params);
    }

    /** {@inheritDoc} */
    @Override
    public List<CompletableFuture<AsyncSqlCursor<List<Object>>>> queryAsync(QueryContext context, String schemaName,
            String qry, Object... params) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(new NodeStoppingException());
        }

        try {
            return query0(context, schemaName, qry, params);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private List<CompletableFuture<AsyncSqlCursor<List<Object>>>> query0(
            QueryContext context,
            String schemaName,
            String sql,
            Object... params
    ) {
        SchemaPlus schema = schemaManager.schema(schemaName);

        if (schema == null) {
            throw new IgniteInternalException(format("Schema not found [schemaName={}]", schemaName));
        }

        SqlNodeList nodes = Commons.parse(sql, FRAMEWORK_CONFIG.getParserConfig());

        var res = new ArrayList<CompletableFuture<AsyncSqlCursor<List<Object>>>>(nodes.size());

        CompletableFuture<Void> start = new CompletableFuture<>();

        for (SqlNode sqlNode : nodes) {
            BaseQueryContext ctx = BaseQueryContext.builder()
                    .cancel(new QueryCancel())
                    .frameworkConfig(
                            Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                    .defaultSchema(schema)
                                    .build()
                    )
                    .logger(LOG)
                    .parameters(params)
                    .build();

            CompletableFuture<AsyncSqlCursor<List<Object>>> stage = start.thenCompose(none -> prepareSvc.prepareAsync(sqlNode, ctx))
                    .thenApply(plan -> {
                        context.maybeUnwrap(QueryValidator.class)
                                .ifPresent(queryValidator -> queryValidator.validatePlan(plan));

                        return new AsyncSqlCursorImpl<>(
                                SqlQueryType.mapPlanTypeToSqlType(plan.type()),
                                plan.metadata(),
                                executionSrvc.executePlan(plan, ctx)
                        );
                    });

            res.add(stage);
        }

        start.completeAsync(() -> null, taskExecutor);

        return res;
    }

    private abstract static class AbstractTableEventListener implements EventListener<TableEventParameters> {
        protected final SqlSchemaManagerImpl schemaHolder;

        private AbstractTableEventListener(
                SqlSchemaManagerImpl schemaHolder
        ) {
            this.schemaHolder = schemaHolder;
        }
    }

    private static class TableCreatedListener extends AbstractTableEventListener {
        private TableCreatedListener(
                SqlSchemaManagerImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onTableCreated(
                    "PUBLIC",
                    parameters.table(),
                    parameters.causalityToken()
            );

            return false;
        }
    }

    private static class TableUpdatedListener extends AbstractTableEventListener {
        private TableUpdatedListener(
                SqlSchemaManagerImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onTableUpdated(
                    "PUBLIC",
                    parameters.table(),
                    parameters.causalityToken()
            );

            return false;
        }
    }

    private static class TableDroppedListener extends AbstractTableEventListener {
        private TableDroppedListener(
                SqlSchemaManagerImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onTableDropped(
                    "PUBLIC",
                    parameters.tableName(),
                    parameters.causalityToken()
            );

            return false;
        }
    }
}
