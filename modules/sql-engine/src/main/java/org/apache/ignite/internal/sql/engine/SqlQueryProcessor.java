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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Pair;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.sql.engine.exec.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistry;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.CacheKey;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlanCache;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlanCacheImpl;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;

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

    private final Consumer<Consumer<Long>> revisionUpdater;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Event listeners to close. */
    private final List<Pair<TableEvent, EventListener<TableEventParameters>>> evtLsnrs = new ArrayList<>();

    private final List<LifecycleAware> services = new ArrayList<>();

    private final TablesConfiguration tablesCfg;

    /** Keeps queries plans to avoid expensive planning of the same queries. */
    private volatile QueryPlanCache planCache;

    private volatile ExecutionService executionSrvc;

    private volatile MessageService msgSrvc;

    private volatile QueryTaskExecutor taskExecutor;

    private volatile PrepareService prepareSvc;

    private volatile MailboxRegistry mailboxRegistry;

    private volatile ExchangeService exchangeService;

    private volatile QueryRegistry queryRegistry;

    private volatile SqlSchemaManager schemaManager;

    /** Constructor. */
    public SqlQueryProcessor(
            TablesConfiguration tablesCfg,
            Consumer<Consumer<Long>> revisionUpdater,
            ClusterService clusterSrvc,
            TableManager tableManager
    ) {
        this.tablesCfg = tablesCfg;
        this.revisionUpdater = revisionUpdater;
        this.clusterSrvc = clusterSrvc;
        this.tableManager = tableManager;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void start() {
        planCache = registerService(new QueryPlanCacheImpl(clusterSrvc.localConfiguration().getName(), PLAN_CACHE_SIZE));
        taskExecutor = registerService(new QueryTaskExecutorImpl(clusterSrvc.localConfiguration().getName()));
        prepareSvc = registerService(new PrepareServiceImpl());
        queryRegistry = registerService(new QueryRegistryImpl());
        mailboxRegistry = registerService(new MailboxRegistryImpl(clusterSrvc.topologyService()));

        msgSrvc = registerService(new MessageServiceImpl(
            clusterSrvc.topologyService(),
            clusterSrvc.messagingService(),
            taskExecutor
        ));

        exchangeService = registerService(new ExchangeServiceImpl(
            clusterSrvc.topologyService().localMember().id(),
            taskExecutor,
            mailboxRegistry,
            msgSrvc,
            queryRegistry
        ));

        SqlSchemaManagerImpl schemaManager = new SqlSchemaManagerImpl(tableManager, revisionUpdater, planCache::clear);

        executionSrvc = registerService(new ExecutionServiceImpl<>(
                clusterSrvc.topologyService(),
                msgSrvc,
                planCache,
                schemaManager,
                tableManager,
                taskExecutor,
                ArrayRowHandler.INSTANCE,
                mailboxRegistry,
                exchangeService,
                queryRegistry
        ));

        this.schemaManager = schemaManager;

        tablesCfg.tables().listenElements(new ConfigurationNamedListListener<TableView>() {
            @Override public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<TableView> ctx) {
                return onTableChange(ctx, (schema, table, token) -> {
                        schemaManager.onTableCreated(schema, table, token);

                        return completedFuture(null);
                });
            }

            @Override public CompletableFuture<?> onRename(String oldName, String newName,
                ConfigurationNotificationEvent<TableView> ctx) {
                return completedFuture(null);
            }

            @Override public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<TableView> ctx) {
                return onTableDrop(ctx);
            }

            @Override public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<TableView> ctx) {
                return onTableChange(ctx, (schema, table, token) -> {
                    schemaManager.onTableUpdated(schema, table, token);

                    return completedFuture(null);
                });
            }
        });

        services.forEach(LifecycleAware::start);
    }

    private CompletableFuture<?> onTableChange(ConfigurationNotificationEvent<TableView> ctx, TableChangeCallback callback) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            String tblName = ctx.newValue().name();

            long causalityToken = ctx.storageRevision();

            return tableManager.table(tblName, causalityToken)
                    .thenApply(table -> callback.onTableChange("PUBLIC", table, causalityToken));
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<?> onTableDrop(ConfigurationNotificationEvent<TableView> ctx) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            String tblName = ctx.oldValue().name();

            long causalityToken = ctx.storageRevision();

            ((SqlSchemaManagerImpl) schemaManager).onTableDropped("PUBLIC", tblName, causalityToken);

            return completedFuture(null);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
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
    public List<SqlCursor<List<?>>> query(String schemaName, String qry, Object... params) {
        return query(QueryContext.of(), schemaName, qry, params);
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlCursor<List<?>>> query(QueryContext context, String schemaName, String qry, Object... params) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return query0(context, schemaName, qry, params);
        } finally {
            busyLock.leaveBusy();
        }
    }

    public QueryRegistry queryRegistry() {
        return queryRegistry;
    }

    private List<SqlCursor<List<?>>> query0(QueryContext context, String schemaName, String sql, Object... params) {
        SchemaPlus schema = schemaManager.schema(schemaName);

        assert schema != null : "Schema not found: " + schemaName;

        QueryPlan plan = planCache.queryPlan(new CacheKey(schema.getName(), sql));

        if (plan != null) {
            final QueryPlan finalPlan = plan;

            context.maybeUnwrap(QueryValidator.class)
                    .ifPresent(queryValidator -> queryValidator.validatePlan(finalPlan));

            RootQuery<Object[]> qry = new RootQuery<>(
                    sql,
                    schema,
                    params,
                    exchangeService,
                    (q) -> queryRegistry.unregister(q.id()),
                    LOG
            );

            queryRegistry.register(qry);

            try {
                return Collections.singletonList(executionSrvc.executePlan(
                        qry,
                        plan
                ));
            } catch (Exception e) {
                boolean isCanceled = qry.isCancelled();

                qry.cancel();

                queryRegistry.unregister(qry.id());

                if (isCanceled) {
                    throw new IgniteInternalException("The query was cancelled while planning", e);
                } else {
                    throw e;
                }
            }
        }

        SqlNodeList qryList = Commons.parse(sql, FRAMEWORK_CONFIG.getParserConfig());
        List<SqlCursor<List<?>>> cursors = new ArrayList<>(qryList.size());

        List<RootQuery<Object[]>> qrys = new ArrayList<>(qryList.size());

        for (final SqlNode sqlNode : qryList) {
            RootQuery<Object[]> qry = new RootQuery<>(
                    sqlNode.toString(),
                    schemaManager.schema(schemaName), // Update schema for each query in multiple statements.
                    params,
                    exchangeService,
                    (q) -> queryRegistry.unregister(q.id()),
                    LOG
            );

            qrys.add(qry);

            queryRegistry.register(qry);

            try {
                if (qryList.size() == 1) {
                    plan = planCache.queryPlan(
                            new CacheKey(schemaName, qry.sql()),
                            () -> prepareSvc.prepareSingle(sqlNode, qry.planningContext()));
                } else {
                    plan = prepareSvc.prepareSingle(sqlNode, qry.planningContext());
                }

                final QueryPlan finalPlan = plan;

                context.maybeUnwrap(QueryValidator.class)
                        .ifPresent(queryValidator -> queryValidator.validatePlan(finalPlan));

                cursors.add(executionSrvc.executePlan(qry, plan));
            } catch (Exception e) {
                boolean isCanceled = qry.isCancelled();

                qrys.forEach(RootQuery::cancel);

                queryRegistry.unregister(qry.id());

                if (isCanceled) {
                    throw new IgniteInternalException("The query was cancelled while planning", e);
                } else {
                    throw e;
                }
            }
        }

        return cursors;
    }

    @FunctionalInterface
    private interface TableChangeCallback {
        CompletableFuture<?> onTableChange(String schemaName, TableImpl table, long causalityToken);
    }
}
