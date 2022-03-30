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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.idx.IndexManager;
import org.apache.ignite.internal.idx.event.IndexEvent;
import org.apache.ignite.internal.idx.event.IndexEventParameters;
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
import org.apache.ignite.internal.sql.engine.extension.SqlExtension;
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

    private final TableManager tblManager;

    private final IndexManager idxManager;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Table event listeners to close. */
    private final List<Pair<TableEvent, EventListener<TableEventParameters>>> tblEvtLsnrs = new ArrayList<>();

    /** Index event listeners to close. */
    private final List<Pair<IndexEvent, EventListener<IndexEventParameters>>> idxEvtLsnrs = new ArrayList<>();

    private final List<LifecycleAware> services = new ArrayList<>();

    /** Keeps queries plans to avoid expensive planning of the same queries. */
    private volatile QueryPlanCache planCache;

    private volatile ExecutionService executionSrvc;

    private volatile MessageService msgSrvc;

    private volatile QueryTaskExecutor taskExecutor;

    private volatile Map<String, SqlExtension> extensions;

    private volatile PrepareService prepareSvc;

    private volatile MailboxRegistry mailboxRegistry;

    private volatile ExchangeService exchangeService;

    private volatile QueryRegistry queryRegistry;

    private volatile SqlSchemaManager schemaManager;

    /**
     * Constructor.
     *
     * @param clusterSrvc Cluster service.
     * @param tblManager Table manager.
     * @param idxManager Index manager.
     */
    public SqlQueryProcessor(
            ClusterService clusterSrvc,
            TableManager tblManager,
            IndexManager idxManager
    ) {
        this.clusterSrvc = clusterSrvc;
        this.tblManager = tblManager;
        this.idxManager = idxManager;
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

        List<SqlExtension> extensionList = new ArrayList<>();

        ServiceLoader<SqlExtension> loader = ServiceLoader.load(SqlExtension.class);

        loader.reload();

        loader.forEach(extensionList::add);

        extensions = extensionList.stream().collect(Collectors.toMap(SqlExtension::name, Function.identity()));

        SqlSchemaManagerImpl schemaManager = new SqlSchemaManagerImpl(tblManager, idxManager, planCache::clear);

        executionSrvc = registerService(new ExecutionServiceImpl<>(
                clusterSrvc.topologyService(),
                msgSrvc,
                planCache,
                schemaManager,
                tblManager,
                idxManager,
                taskExecutor,
                ArrayRowHandler.INSTANCE,
                mailboxRegistry,
                exchangeService,
                queryRegistry,
                extensions
        ));

        registerTableListener(TableEvent.CREATE, new TableCreatedListener(schemaManager));
        registerTableListener(TableEvent.ALTER, new TableUpdatedListener(schemaManager));
        registerTableListener(TableEvent.DROP, new TableDroppedListener(schemaManager));

        registerIndexListener(IndexEvent.CREATE, new IndexCreatedListener(schemaManager));
        registerIndexListener(IndexEvent.DROP, new IndexDroppedListener(schemaManager));

        this.schemaManager = schemaManager;

        services.forEach(LifecycleAware::start);

        extensionList.forEach(ext -> ext.init(catalog -> schemaManager.registerExternalCatalog(ext.name(), catalog)));
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
    }

    private void registerTableListener(TableEvent evt, AbstractTableEventListener lsnr) {
        tblEvtLsnrs.add(Pair.of(evt, lsnr));

        tblManager.listen(evt, lsnr);
    }

    private void registerIndexListener(IndexEvent evt, AbstractIndexEventListener lsnr) {
        idxEvtLsnrs.add(Pair.of(evt, lsnr));

        idxManager.listen(evt, lsnr);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void stop() throws Exception {
        busyLock.block();

        List<AutoCloseable> toClose = new ArrayList<>();

        Map<String, SqlExtension> extensions = this.extensions;
        if (extensions != null) {
            toClose.addAll(
                    extensions.values().stream()
                            .map(ext -> (AutoCloseable) ext::stop)
                            .collect(Collectors.toList())
            );
        }

        List<LifecycleAware> services = new ArrayList<>(this.services);

        this.services.clear();

        Collections.reverse(services);

        Stream<AutoCloseable> closableComponents = services.stream().map(s -> s::stop);

        Stream<AutoCloseable> closableListeners = Stream.concat(
                tblEvtLsnrs.stream()
                    .map((p) -> () -> tblManager.removeListener(p.left, p.right)),
                idxEvtLsnrs.stream()
                        .map((p) -> () -> idxManager.removeListener(p.left, p.right))
        );

        toClose.addAll(
                Stream.concat(closableComponents, closableListeners).collect(Collectors.toList())
        );

        IgniteUtils.closeAll(toClose);
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlCursor<List<?>>> query(String schemaName, String qry, Object... params) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return query0(schemaName, qry, params);
        } finally {
            busyLock.leaveBusy();
        }
    }

    public QueryRegistry queryRegistry() {
        return queryRegistry;
    }

    private List<SqlCursor<List<?>>> query0(String schemaName, String sql, Object... params) {
        SchemaPlus schema = schemaManager.schema(schemaName);

        assert schema != null : "Schema not found: " + schemaName;

        QueryPlan plan = planCache.queryPlan(new CacheKey(schema.getName(), sql));

        if (plan != null) {
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

    private abstract static class AbstractTableEventListener implements EventListener<TableEventParameters> {
        protected final SqlSchemaManagerImpl schemaHolder;

        private AbstractTableEventListener(SqlSchemaManagerImpl schemaHolder) {
            this.schemaHolder = schemaHolder;
        }

        /** {@inheritDoc} */
        @Override
        public void remove(@NotNull Throwable exception) {
            // No-op.
        }
    }

    private static class TableCreatedListener extends AbstractTableEventListener {
        private TableCreatedListener(SqlSchemaManagerImpl schemaHolder) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            if (exception != null) {
                return false;
            }

            schemaHolder.onTableCreated(
                    "PUBLIC",
                    parameters.table()
            );

            return false;
        }
    }

    private static class TableUpdatedListener extends AbstractTableEventListener {
        private TableUpdatedListener(SqlSchemaManagerImpl schemaHolder) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            if (exception != null) {
                return false;
            }

            schemaHolder.onTableUpdated(
                    "PUBLIC",
                    parameters.table()
            );

            return false;
        }
    }

    private static class TableDroppedListener extends AbstractTableEventListener {
        private TableDroppedListener(SqlSchemaManagerImpl schemaHolder) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            if (exception != null) {
                return false;
            }

            schemaHolder.onTableDropped(
                    "PUBLIC",
                    parameters.tableName()
            );

            return false;
        }
    }

    private abstract static class AbstractIndexEventListener implements EventListener<IndexEventParameters> {
        protected final SqlSchemaManagerImpl schemaHolder;

        private AbstractIndexEventListener(SqlSchemaManagerImpl schemaHolder) {
            this.schemaHolder = schemaHolder;
        }

        /** {@inheritDoc} */
        @Override
        public void remove(@NotNull Throwable exception) {
            // No-op.
        }
    }

    private static class IndexCreatedListener extends AbstractIndexEventListener {
        private IndexCreatedListener(SqlSchemaManagerImpl schemaHolder) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull IndexEventParameters parameters, @Nullable Throwable exception) {
            if (exception != null) {
                return false;
            }

            schemaHolder.onIndexCreated(
                    "PUBLIC",
                    parameters.tableName(),
                    parameters.index()
            );

            return false;
        }
    }

    private static class IndexDroppedListener extends AbstractIndexEventListener {
        private IndexDroppedListener(SqlSchemaManagerImpl schemaHolder) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override
        public boolean notify(@NotNull IndexEventParameters parameters, @Nullable Throwable exception) {
            if (exception != null) {
                return false;
            }

            schemaHolder.onIndexDropped(
                    "PUBLIC",
                    parameters.tableName(),
                    parameters.indexName()
            );

            return false;
        }
    }
}
