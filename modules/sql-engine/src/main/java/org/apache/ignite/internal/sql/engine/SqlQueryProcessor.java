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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.Event;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.sql.engine.exec.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutableTableRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionDependencyResolverImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.exec.QueryValidationException;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandlerWrapper;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.property.PropertiesHelper;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.schema.CatalogSqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.session.Session;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.sql.engine.session.SessionInfo;
import org.apache.ignite.internal.sql.engine.session.SessionManager;
import org.apache.ignite.internal.sql.engine.session.SessionNotFoundException;
import org.apache.ignite.internal.sql.engine.session.SessionProperty;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.CaffeineCacheFactory;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.SchemaNotFoundException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * SqlQueryProcessor.
 *  TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class SqlQueryProcessor implements QueryProcessor {
    private static final IgniteLogger LOG = Loggers.forClass(SqlQueryProcessor.class);

    /** Size of the cache for query plans. */
    private static final int PLAN_CACHE_SIZE = 1024;

    private static final int PARSED_RESULT_CACHE_SIZE = 10_000;

    /** Size of the table access cache. */
    private static final int TABLE_CACHE_SIZE = 1024;

    /** Session expiration check period in milliseconds. */
    private static final long SESSION_EXPIRE_CHECK_PERIOD = TimeUnit.SECONDS.toMillis(1);

    /**
     * Duration in milliseconds after which the session will be considered expired if no action have been performed
     * on behalf of this session during this period.
     */
    private static final long DEFAULT_SESSION_IDLE_TIMEOUT = TimeUnit.MINUTES.toMillis(15);

    /** Name of the default schema. */
    public static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    private static final PropertiesHolder DEFAULT_PROPERTIES = PropertiesHelper.newBuilder()
            .set(QueryProperty.DEFAULT_SCHEMA, DEFAULT_SCHEMA_NAME)
            .set(SessionProperty.IDLE_TIMEOUT, DEFAULT_SESSION_IDLE_TIMEOUT)
            .build();

    private final ParserService parserService = new ParserServiceImpl(
            PARSED_RESULT_CACHE_SIZE, CaffeineCacheFactory.INSTANCE
    );

    private final List<LifecycleAware> services = new ArrayList<>();

    private final ClusterService clusterSrvc;

    private final TableManager tableManager;

    private final IndexManager indexManager;

    private final SchemaManager schemaManager;

    private final DataStorageManager dataStorageManager;

    private final Supplier<Map<String, Map<String, Class<?>>>> dataStorageFieldsSupplier;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Event listeners to close. */
    private final List<Pair<Event, EventListener>> evtLsnrs = new ArrayList<>();

    private final ReplicaService replicaService;

    private volatile SessionManager sessionManager;

    private volatile QueryTaskExecutor taskExecutor;

    private volatile ExecutionService executionSrvc;

    private volatile PrepareService prepareSvc;

    private volatile SqlSchemaManager sqlSchemaManager;

    /** Transaction manager. */
    private final TxManager txManager;

    /** Distribution zones manager. */
    private final DistributionZoneManager distributionZoneManager;

    /** Clock. */
    private final HybridClock clock;

    /** Distributed catalog manager. */
    private final CatalogManager catalogManager;

    /** Constructor. */
    public SqlQueryProcessor(
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            ClusterService clusterSrvc,
            TableManager tableManager,
            IndexManager indexManager,
            SchemaManager schemaManager,
            DataStorageManager dataStorageManager,
            TxManager txManager,
            DistributionZoneManager distributionZoneManager,
            Supplier<Map<String, Map<String, Class<?>>>> dataStorageFieldsSupplier,
            ReplicaService replicaService,
            HybridClock clock,
            CatalogManager catalogManager
    ) {
        this.clusterSrvc = clusterSrvc;
        this.tableManager = tableManager;
        this.indexManager = indexManager;
        this.schemaManager = schemaManager;
        this.dataStorageManager = dataStorageManager;
        this.txManager = txManager;
        this.distributionZoneManager = distributionZoneManager;
        this.dataStorageFieldsSupplier = dataStorageFieldsSupplier;
        this.replicaService = replicaService;
        this.clock = clock;
        this.catalogManager = catalogManager;

        sqlSchemaManager = new CatalogSqlSchemaManager(
                catalogManager,
                0 // TOOD fix cache size
        );
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void start() {
        var nodeName = clusterSrvc.topologyService().localMember().name();

        sessionManager = registerService(new SessionManager(nodeName, SESSION_EXPIRE_CHECK_PERIOD, System::currentTimeMillis));

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
                taskExecutor,
                busyLock
        ));

        var exchangeService = registerService(new ExchangeServiceImpl(
                mailboxRegistry,
                msgSrvc
        ));

        this.prepareSvc = prepareSvc;

        var ddlCommandHandler = new DdlCommandHandlerWrapper(
                distributionZoneManager,
                tableManager,
                indexManager,
                dataStorageManager,
                catalogManager
        );

        var executableTableRegistry = new ExecutableTableRegistryImpl(tableManager, schemaManager, replicaService, clock, TABLE_CACHE_SIZE);

        var dependencyResolver = new ExecutionDependencyResolverImpl(executableTableRegistry);

        var executionSrvc = registerService(ExecutionServiceImpl.create(
                clusterSrvc.topologyService(),
                msgSrvc,
                sqlSchemaManager,
                ddlCommandHandler,
                taskExecutor,
                ArrayRowHandler.INSTANCE,
                mailboxRegistry,
                exchangeService,
                dependencyResolver
        ));

        clusterSrvc.topologyService().addEventHandler(executionSrvc);
        clusterSrvc.topologyService().addEventHandler(mailboxRegistry);

        this.executionSrvc = executionSrvc;

        services.forEach(LifecycleAware::start);
    }

    /** {@inheritDoc} */
    @Override
    public SessionId createSession(PropertiesHolder properties) {
        properties = PropertiesHelper.merge(properties, DEFAULT_PROPERTIES);

        return sessionManager.createSession(
                properties.get(SessionProperty.IDLE_TIMEOUT),
                properties
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeSession(SessionId sessionId) {
        var session = sessionManager.session(sessionId);

        if (session == null) {
            return CompletableFuture.completedFuture(null);
        }

        return session.closeAsync();
    }

    /** {@inheritDoc} */
    @Override
    public List<SessionInfo> liveSessions() {
        return sessionManager.liveSessions();
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
                .map((p) -> () -> {
                    if (p.left instanceof TableEvent) {
                        tableManager.removeListener((TableEvent) p.left, p.right);
                    } else {
                        indexManager.removeListener((IndexEvent) p.left, p.right);
                    }
                });

        IgniteUtils.closeAll(Stream.concat(closableComponents, closableListeners).collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncSqlCursor<List<Object>>> querySingleAsync(
            SessionId sessionId, QueryContext context, String qry, Object... params
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            return querySingle0(sessionId, context, qry, params);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private <T extends LifecycleAware> T registerService(T service) {
        services.add(service);

        return service;
    }

    private CompletableFuture<AsyncSqlCursor<List<Object>>> querySingle0(
            SessionId sessionId,
            QueryContext context,
            String sql,
            Object... params
    ) {
        Session session = sessionManager.session(sessionId);

        if (session == null) {
            return CompletableFuture.failedFuture(new SessionNotFoundException(sessionId));
        }

        String schemaName = session.properties().get(QueryProperty.DEFAULT_SCHEMA);

        InternalTransaction outerTx = context.unwrap(InternalTransaction.class);

        QueryCancel queryCancel = new QueryCancel();

        AsyncCloseable closeableResource = () -> CompletableFuture.runAsync(
                queryCancel::cancel,
                taskExecutor
        );

        queryCancel.add(() -> session.unregisterResource(closeableResource));

        try {
            session.registerResource(closeableResource);
        } catch (IllegalStateException ex) {
            return CompletableFuture.failedFuture(new SessionNotFoundException(sessionId));
        }

        CompletableFuture<Void> start = new CompletableFuture<>();

        AtomicReference<InternalTransaction> tx = new AtomicReference<>();

        CompletableFuture<AsyncSqlCursor<List<Object>>> stage = start
                .thenCompose(ignored -> {
                    ParsedResult result = parserService.parse(sql);

                    validateParsedStatement(context, outerTx, result, params);

                    boolean rwOp = dataModificationOp(result);

                    boolean implicitTxRequired = outerTx == null;

                    tx.set(implicitTxRequired ? txManager.begin(!rwOp, null) : outerTx);

                    int catalogVersion = catalogManager.activeCatalogVersion(tx.get().startTimestamp().longValue());

                    SchemaPlus schema = sqlSchemaManager.schema(schemaName, catalogVersion);

                    if (schema == null) {
                        return CompletableFuture.failedFuture(new SchemaNotFoundException(schemaName));
                    }

                    BaseQueryContext ctx = BaseQueryContext.builder()
                            .frameworkConfig(
                                    Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                            .defaultSchema(schema)
                                            .build()
                            )
                            .logger(LOG)
                            .cancel(queryCancel)
                            .parameters(params)
                            .build();

                    return prepareSvc.prepareAsync(result, ctx)
                            .thenApply(plan -> {
                                var dataCursor = executionSrvc.executePlan(tx.get(), plan, ctx);

                                SqlQueryType queryType = plan.type();
                                assert queryType != null : "Expected a full plan but got a fragment: " + plan;

                                return new AsyncSqlCursorImpl<>(
                                        queryType,
                                        plan.metadata(),
                                        implicitTxRequired ? tx.get() : null,
                                        new AsyncCursor<List<Object>>() {
                                            @Override
                                            public CompletableFuture<BatchedResult<List<Object>>> requestNextAsync(int rows) {
                                                session.touch();

                                                return dataCursor.requestNextAsync(rows);
                                            }

                                            @Override
                                            public CompletableFuture<Void> closeAsync() {
                                                session.touch();

                                                return dataCursor.closeAsync();
                                            }
                                        }
                                );
                            });
                });

        stage.whenComplete((cur, ex) -> {
            if (ex instanceof CancellationException) {
                queryCancel.cancel();
            }

            if (ex != null && outerTx == null) {
                InternalTransaction tx0 = tx.get();
                if (tx0 != null) {
                    tx0.rollback();
                }
            }
        });

        start.completeAsync(() -> null, taskExecutor);

        return stage;
    }

    /** Returns {@code true} if this is data modification operation. */
    private static boolean dataModificationOp(ParsedResult parsedResult) {
        return parsedResult.queryType() == SqlQueryType.DML;
    }

    /** Performs additional validation of a parsed statement. **/
    private static void validateParsedStatement(
            QueryContext context,
            @Nullable InternalTransaction outerTx,
            ParsedResult parsedResult,
            Object[] params
    ) {
        Set<SqlQueryType> allowedTypes = context.allowedQueryTypes();
        SqlQueryType queryType = parsedResult.queryType();

        if (!allowedTypes.contains(queryType)) {
            String message = format("Invalid SQL statement type in the batch. Expected {} but got {}.", allowedTypes, queryType);

            throw new QueryValidationException(message);
        }

        if (SqlQueryType.DDL == queryType && outerTx != null) {
            throw new SqlException(STMT_VALIDATION_ERR, "DDL doesn't support transactions.");
        }

        if (parsedResult.dynamicParamsCount() != params.length) {
            String message = format(
                    "Unexpected number of query parameters. Provided {} but there is only {} dynamic parameter(s).",
                    params.length, parsedResult.dynamicParamsCount()
            );

            throw new SqlException(STMT_VALIDATION_ERR, message);
        }

        for (Object param : params) {
            if (!TypeUtils.supportParamInstance(param)) {
                String message = format(
                        "Unsupported dynamic parameter defined. Provided '{}' is not supported.", param.getClass().getName());

                throw new SqlException(STMT_VALIDATION_ERR, message);
            }
        }
    }
}
