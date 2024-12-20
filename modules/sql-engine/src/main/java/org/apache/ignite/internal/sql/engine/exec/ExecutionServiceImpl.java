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

package org.apache.ignite.internal.sql.engine.exec;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.externalize.RelJsonReader.fromJson;
import static org.apache.ignite.internal.sql.engine.util.Commons.cast;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import com.github.benmanes.caffeine.cache.Caffeine;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.InternalSqlRowImpl;
import org.apache.ignite.internal.sql.engine.InternalSqlRowSingleBoolean;
import org.apache.ignite.internal.sql.engine.InternalSqlRowSingleString;
import org.apache.ignite.internal.sql.engine.NodeLeftException;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor.PrefetchCallback;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursorExt.CancellationReason;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionRegistry;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommand;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappedFragment;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingParameters;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingService;
import org.apache.ignite.internal.sql.engine.exec.rel.AbstractNode;
import org.apache.ignite.internal.sql.engine.exec.rel.AsyncRootNode;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.sql.engine.message.ErrorMessage;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.QueryCloseMessage;
import org.apache.ignite.internal.sql.engine.message.QueryStartRequest;
import org.apache.ignite.internal.sql.engine.message.QueryStartResponse;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.sql.engine.prepare.DdlPlan;
import org.apache.ignite.internal.sql.engine.prepare.ExplainPlan;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.prepare.KillPlan;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.SourceAwareIgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.tx.NoopTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IteratorToDataCursorAdapter;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Provide ability to execute SQL query plan and retrieve results of the execution.
 */
public class ExecutionServiceImpl<RowT> implements ExecutionService, TopologyEventHandler {
    private static final int CACHE_SIZE = 1024;
    private static final IgniteLogger LOG = Loggers.forClass(ExecutionServiceImpl.class);
    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();
    private static final List<InternalSqlRow> APPLIED_ANSWER = List.of(new InternalSqlRowSingleBoolean(true));
    private static final List<InternalSqlRow> NOT_APPLIED_ANSWER = List.of(new InternalSqlRowSingleBoolean(false));
    private static final FragmentDescription DUMMY_DESCRIPTION = new FragmentDescription(
            0, true, Long2ObjectMaps.emptyMap(), null, null, null
    );

    private final ConcurrentMap<FragmentCacheKey, IgniteRel> physNodesCache = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .<FragmentCacheKey, IgniteRel>build()
            .asMap();

    private final AtomicInteger executionTokenGen = new AtomicInteger();

    private final MessageService messageService;

    private final TopologyService topSrvc;

    private final ClusterNode localNode;

    private final SqlSchemaManager sqlSchemaManager;

    private final QueryTaskExecutor taskExecutor;

    private final MappingService mappingService;

    private final RowHandler<RowT> handler;

    private final DdlCommandHandler ddlCmdHnd;

    private final ExecutionDependencyResolver dependencyResolver;

    private final ExecutableTableRegistry tableRegistry;

    private final ImplementorFactory<RowT> implementorFactory;

    private final Map<ExecutionId, DistributedQueryManager> queryManagerMap = new ConcurrentHashMap<>();

    private final long shutdownTimeout;

    private final ClockService clockService;

    private final KillCommandHandler killCommandHandler;

    /**
     * Constructor.
     *
     * @param messageService Message service.
     * @param topSrvc Topology service.
     * @param mappingService Nodes mapping calculation service.
     * @param sqlSchemaManager Schema manager.
     * @param ddlCmdHnd Handler of the DDL commands.
     * @param taskExecutor Task executor.
     * @param handler Row handler.
     * @param implementorFactory Relational node implementor factory.
     * @param clockService Clock service.
     * @param killCommandHandler Kill command handler.
     * @param shutdownTimeout Shutdown timeout.
     */
    public ExecutionServiceImpl(
            MessageService messageService,
            TopologyService topSrvc,
            MappingService mappingService,
            SqlSchemaManager sqlSchemaManager,
            DdlCommandHandler ddlCmdHnd,
            QueryTaskExecutor taskExecutor,
            RowHandler<RowT> handler,
            ExecutableTableRegistry tableRegistry,
            ExecutionDependencyResolver dependencyResolver,
            ImplementorFactory<RowT> implementorFactory,
            ClockService clockService,
            KillCommandHandler killCommandHandler,
            long shutdownTimeout
    ) {
        this.localNode = topSrvc.localMember();
        this.handler = handler;
        this.messageService = messageService;
        this.mappingService = mappingService;
        this.topSrvc = topSrvc;
        this.sqlSchemaManager = sqlSchemaManager;
        this.taskExecutor = taskExecutor;
        this.ddlCmdHnd = ddlCmdHnd;
        this.tableRegistry = tableRegistry;
        this.dependencyResolver = dependencyResolver;
        this.implementorFactory = implementorFactory;
        this.clockService = clockService;
        this.killCommandHandler = killCommandHandler;
        this.shutdownTimeout = shutdownTimeout;
    }

    /**
     * Creates the execution services.
     *
     * @param <RowT> Type of the sql row.
     * @param topSrvc Topology service.
     * @param msgSrvc Message service.
     * @param sqlSchemaManager Schema manager.
     * @param ddlCommandHandler Handler of the DDL commands.
     * @param taskExecutor Task executor.
     * @param handler Row handler.
     * @param mailboxRegistry Mailbox registry.
     * @param exchangeSrvc Exchange service.
     * @param mappingService Nodes mapping calculation service.
     * @param tableRegistry Table registry.
     * @param dependencyResolver Dependency resolver.
     * @param tableFunctionRegistry Table function registry.
     * @param clockService Clock service.
     * @param killCommandHandler Kill command handler.
     * @param shutdownTimeout Shutdown timeout.
     * @return An execution service.
     */
    public static <RowT> ExecutionServiceImpl<RowT> create(
            TopologyService topSrvc,
            MessageService msgSrvc,
            SqlSchemaManager sqlSchemaManager,
            DdlCommandHandler ddlCommandHandler,
            QueryTaskExecutor taskExecutor,
            RowHandler<RowT> handler,
            MailboxRegistry mailboxRegistry,
            ExchangeService exchangeSrvc,
            MappingService mappingService,
            ExecutableTableRegistry tableRegistry,
            ExecutionDependencyResolver dependencyResolver,
            TableFunctionRegistry tableFunctionRegistry,
            ClockService clockService,
            KillCommandHandler killCommandHandler,
            long shutdownTimeout
    ) {
        return new ExecutionServiceImpl<>(
                msgSrvc,
                topSrvc,
                mappingService,
                sqlSchemaManager,
                ddlCommandHandler,
                taskExecutor,
                handler,
                tableRegistry,
                dependencyResolver,
                (ctx, deps) -> new LogicalRelImplementor<>(
                        ctx,
                        mailboxRegistry,
                        exchangeSrvc,
                        deps,
                        tableFunctionRegistry
                ),
                clockService,
                killCommandHandler,
                shutdownTimeout
        );
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        messageService.register((n, m) -> onMessage(n, (QueryStartRequest) m), SqlQueryMessageGroup.QUERY_START_REQUEST);
        messageService.register((n, m) -> onMessage(n, (QueryStartResponse) m), SqlQueryMessageGroup.QUERY_START_RESPONSE);
        messageService.register((n, m) -> onMessage(n, (QueryCloseMessage) m), SqlQueryMessageGroup.QUERY_CLOSE_MESSAGE);
        messageService.register((n, m) -> onMessage(n, (ErrorMessage) m), SqlQueryMessageGroup.ERROR_MESSAGE);
    }

    @TestOnly
    public ExecutableTableRegistry tableRegistry() {
        return tableRegistry;
    }

    @TestOnly
    public DdlCommandHandler ddlCommandHandler() {
        return ddlCmdHnd;
    }

    private CompletableFuture<AsyncDataCursorExt<InternalSqlRow>> executeQuery(
            SqlOperationContext operationContext,
            MultiStepPlan plan
    ) {
        ExecutionId executionid = nextExecutionId(operationContext.queryId());
        DistributedQueryManager queryManager = new DistributedQueryManager(executionid, localNode.name(), true, operationContext);

        DistributedQueryManager old = queryManagerMap.put(executionid, queryManager);

        assert old == null;

        QueryCancel cancelHandler = operationContext.cancel();

        assert cancelHandler != null;

        // This call immediately triggers a cancellation exception if operation has timed out or it has already been cancelled.
        cancelHandler.add(timeout -> {
            CancellationReason reason = timeout ? CancellationReason.TIMEOUT : CancellationReason.CANCEL;
            queryManager.close(reason);
        });

        QueryTransactionContext txContext = operationContext.txContext();

        assert txContext != null;

        QueryTransactionWrapper txWrapper = txContext.getOrStartImplicit(plan.type() != SqlQueryType.DML);
        InternalTransaction tx = txWrapper.unwrap();

        operationContext.notifyTxUsed(txWrapper);

        PrefetchCallback prefetchCallback = queryManager.prefetchCallback;

        CompletableFuture<Void> firstPageReady = prefetchCallback.prefetchFuture();

        if (plan.type() == SqlQueryType.DML) {
            // DML is supposed to have a single row response, so if the first page is ready, then all
            // inputs have been processed, all tables have been updated, and now it should be safe to
            // commit implicit transaction
            firstPageReady = firstPageReady.thenCompose(none -> txWrapper.commitImplicit());
        }

        CompletableFuture<Void> firstPageReady0 = firstPageReady;

        Predicate<String> nodeExclusionFilter = operationContext.nodeExclusionFilter();

        CompletableFuture<AsyncDataCursorExt<InternalSqlRow>> f = queryManager.execute(tx, plan,
                nodeExclusionFilter).thenApply(dataCursor -> new TxAwareAsyncCursor<>(
                txWrapper,
                dataCursor,
                firstPageReady0,
                queryManager::close
        ));

        return f.whenComplete((r, t) -> {
            if (t != null) {
                txWrapper.rollback(t);
            }
        });
    }

    private static SqlOperationContext createOperationContext(
            UUID queryId, ZoneId timeZoneId, Object[] params, HybridTimestamp operationTime
    ) {
        return SqlOperationContext.builder()
                .queryId(queryId)
                .parameters(params)
                .timeZoneId(timeZoneId)
                .operationTime(operationTime)
                .build();
    }

    private IgniteRel relationalTreeFromJsonString(int catalogVersion, String jsonFragment) {
        SchemaPlus rootSchema = sqlSchemaManager.schema(catalogVersion);

        return physNodesCache.computeIfAbsent(
                new FragmentCacheKey(catalogVersion, jsonFragment),
                key -> fromJson(rootSchema, key.fragmentString)
        );
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("CastConflictsWithInstanceof") // IDEA incorrectly highlights casts in EXPLAIN and DDL branches
    public CompletableFuture<AsyncDataCursorExt<InternalSqlRow>> executePlan(
            QueryPlan plan, SqlOperationContext operationContext
    ) {
        SqlQueryType queryType = plan.type();

        switch (queryType) {
            case DML:
            case QUERY:
                if (plan instanceof ExecutablePlan) {
                    return cast(completedFuture(executeExecutablePlan(operationContext, (ExecutablePlan) plan)));
                }

                assert plan instanceof MultiStepPlan : plan.getClass();

                return executeQuery(operationContext, (MultiStepPlan) plan);
            case EXPLAIN:
                return cast(completedFuture(executeExplain((ExplainPlan) plan)));
            case DDL:
                return cast(completedFuture(executeDdl(operationContext, (DdlPlan) plan)));
            case KILL:
                return cast(completedFuture(executeKill(operationContext, (KillPlan) plan)));

            default:
                throw new AssertionError("Unexpected query type: " + plan);
        }
    }

    private AsyncDataCursor<InternalSqlRow> executeExecutablePlan(
            SqlOperationContext operationContext,
            ExecutablePlan plan
    ) {
        QueryCancel queryCancel = operationContext.cancel();
        assert queryCancel != null;

        ExecutionId executionId = nextExecutionId(operationContext.queryId());
        ExecutionContext<RowT> ectx = new ExecutionContext<>(
                taskExecutor,
                executionId,
                localNode,
                localNode.name(),
                DUMMY_DESCRIPTION,
                handler,
                Commons.parametersMap(operationContext.parameters()),
                TxAttributes.dummy(),
                operationContext.timeZoneId(),
                operationContext.cancel()
        );

        QueryTransactionContext txContext = operationContext.txContext();

        assert txContext != null;

        QueryTransactionWrapper txWrapper = txContext.explicitTx();

        if (txWrapper == null) {
            // underlying table will initiate transaction by itself, but we need stub to reuse
            // TxAwareAsyncCursor
            txWrapper = NoopTransactionWrapper.INSTANCE;
        }

        PrefetchCallback prefetchCallback = new PrefetchCallback();

        AsyncCursor<InternalSqlRow> dataCursor = plan.execute(ectx, txWrapper.unwrap(), tableRegistry, prefetchCallback);

        return new TxAwareAsyncCursor<>(
                txWrapper,
                dataCursor,
                prefetchCallback.prefetchFuture(),
                reason -> nullCompletedFuture()
        );
    }

    private AsyncDataCursor<InternalSqlRow> executeDdl(SqlOperationContext operationContext,
            DdlPlan plan
    ) {
        CompletableFuture<Iterator<InternalSqlRow>> ret = ddlCmdHnd.handle(plan.command())
                .thenApply(applied -> (applied ? APPLIED_ANSWER : NOT_APPLIED_ANSWER).iterator())
                .exceptionally(th -> {
                    throw convertDdlException(th);
                });

        QueryCancel queryCancel = operationContext.cancel();
        assert queryCancel != null;

        queryCancel.add(timeout -> {
            if (timeout) {
                ret.completeExceptionally(new QueryCancelledException(QueryCancelledException.TIMEOUT_MSG));
            }
        });

        return new IteratorToDataCursorAdapter<>(ret, Runnable::run);
    }

    private AsyncDataCursor<InternalSqlRow> executeKill(
            SqlOperationContext operationContext,
            KillPlan plan
    ) {
        KillCommand cmd = plan.command();

        CompletableFuture<Iterator<InternalSqlRow>> ret = killCommandHandler.handle(cmd)
                .thenApply(cancelled -> (cancelled ? APPLIED_ANSWER : NOT_APPLIED_ANSWER).iterator())
                .exceptionally(th -> {
                    Throwable e = ExceptionUtils.unwrapCause(th);

                    if (e instanceof IgniteInternalCheckedException) {
                        throw new IgniteInternalException(INTERNAL_ERR, "Failed to execute KILL statement"
                                + " [command=" + cmd + ", err=" + e.getMessage() + ']', e);
                    }

                    throw (e instanceof RuntimeException) ? (RuntimeException) e : new IgniteInternalException(INTERNAL_ERR, e);
                });

        QueryCancel queryCancel = operationContext.cancel();
        assert queryCancel != null;

        queryCancel.add(timeout -> {
            if (timeout) {
                ret.completeExceptionally(new QueryCancelledException(QueryCancelledException.TIMEOUT_MSG));
            }
        });

        return new IteratorToDataCursorAdapter<>(ret, Runnable::run);
    }

    private static RuntimeException convertDdlException(Throwable e) {
        e = ExceptionUtils.unwrapCause(e);

        if (e instanceof ConfigurationChangeException) {
            assert e.getCause() != null;
            // Cut off upper configuration error`s as uninformative.
            e = e.getCause();
        }

        if (e instanceof IgniteInternalCheckedException) {
            return new IgniteInternalException(INTERNAL_ERR, "Failed to execute DDL statement [stmt=" /* + qry.sql() */
                    + ", err=" + e.getMessage() + ']', e);
        }

        return (e instanceof RuntimeException) ? (RuntimeException) e : new IgniteInternalException(INTERNAL_ERR, e);
    }

    @SuppressWarnings("MethodMayBeStatic")
    private AsyncDataCursor<InternalSqlRow> executeExplain(ExplainPlan plan) {
        String planString = plan.plan().explain();

        InternalSqlRow res = new InternalSqlRowSingleString(planString);

        return new IteratorToDataCursorAdapter<>(List.of(res).iterator());
    }

    private void onMessage(String nodeName, QueryStartRequest msg) {
        assert nodeName != null && msg != null;

        CompletableFuture<Void> fut = sqlSchemaManager.schemaReadyFuture(msg.catalogVersion());

        if (fut.isDone()) {
            submitFragment(nodeName, msg);
        } else {
            fut.whenComplete((mgr, ex) -> {
                if (ex != null) {
                    handleError(ex, nodeName, msg);
                    return;
                }

                taskExecutor.execute(msg.queryId(), msg.fragmentId(), () -> submitFragment(nodeName, msg));
            });
        }
    }

    private void onMessage(String nodeName, QueryStartResponse msg) {
        assert nodeName != null && msg != null;

        DistributedQueryManager dqm = queryManagerMap.get(new ExecutionId(msg.queryId(), msg.executionToken()));

        if (dqm != null) {
            dqm.acknowledgeFragment(nodeName, msg.fragmentId(), msg.error());
        }
    }

    private void onMessage(String nodeName, ErrorMessage msg) {
        assert nodeName != null && msg != null;

        DistributedQueryManager dqm = queryManagerMap.get(new ExecutionId(msg.queryId(), msg.executionToken()));

        if (dqm != null) {
            RemoteFragmentExecutionException e = new RemoteFragmentExecutionException(
                    nodeName,
                    msg.queryId(),
                    msg.fragmentId(),
                    msg.traceId(),
                    msg.code(),
                    msg.message()
            );
            if (LOG.isDebugEnabled()) {
                LOG.debug("Query remote fragment execution failed [nodeName={}, queryId={}, fragmentId={}, originalMessage={}]",
                        nodeName, e.queryId(), e.fragmentId(), e.getMessage());
            }

            dqm.onError(e);
        }
    }

    private void onMessage(String nodeName, QueryCloseMessage msg) {
        assert nodeName != null && msg != null;

        DistributedQueryManager dqm = queryManagerMap.get(new ExecutionId(msg.queryId(), msg.executionToken()));

        if (dqm != null) {
            dqm.close(CancellationReason.CANCEL);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        CompletableFuture<Void> f = CompletableFuture.allOf(queryManagerMap.values().stream()
                .filter(mgr -> mgr.rootFragmentId != null)
                .map(mgr -> mgr.close(CancellationReason.CANCEL))
                .toArray(CompletableFuture[]::new)
        );

        try {
            // If the service was unable to stop within the provided timeout, debugging information will be printed to the log.
            f.get(shutdownTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            String message = format("SQL execution service could not be stopped within the specified timeout ({} ms).", shutdownTimeout);

            LOG.warn(message + dumpDebugInfo() + dumpThreads());
        } catch (CancellationException e) {
            LOG.warn("The stop future was cancelled, going to proceed the stop procedure", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            LOG.warn("The stop future was interrupted, going to proceed the stop procedure", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onDisappeared(ClusterNode member) {
        queryManagerMap.values().forEach(qm -> qm.onNodeLeft(member.name()));
    }

    /** Returns local fragments for the query with given id. */
    @TestOnly
    public List<AbstractNode<?>> localFragments(UUID queryId) {
        return queryManagerMap.entrySet().stream()
                .filter(e -> e.getKey().queryId().equals(queryId))
                .flatMap(e -> e.getValue().localFragments().stream())
                .collect(Collectors.toList());
    }

    private void submitFragment(String nodeName, QueryStartRequest msg) {
        DistributedQueryManager queryManager = getOrCreateQueryManager(nodeName, msg);

        queryManager.submitFragment(nodeName, msg.catalogVersion(), msg.root(), msg.fragmentDescription(), msg.txAttributes());
    }

    private void handleError(Throwable ex, String nodeName, QueryStartRequest msg) {
        DistributedQueryManager queryManager = getOrCreateQueryManager(nodeName, msg);

        queryManager.handleError(ex, nodeName, msg.fragmentDescription().fragmentId());
    }

    private DistributedQueryManager getOrCreateQueryManager(String coordinatorNodeName, QueryStartRequest msg) {
        return queryManagerMap.computeIfAbsent(new ExecutionId(msg.queryId(), msg.executionToken()), key -> {
            SqlOperationContext operationContext = createOperationContext(
                    key.queryId(), ZoneId.of(msg.timeZoneId()), msg.parameters(), msg.operationTime()
            );

            return new DistributedQueryManager(key, coordinatorNodeName, operationContext);
        });
    }

    /**
     * Collects debug information about current state of execution.
     *
     * <p>This information has the following format:
     *
     * <pre>
     * Debug info for query: [uuid] (canceled=[true/false], stopped=[true/false])
     *   Coordinator node: [nodeName] [(current node)]
     *   Root node state: [opened/closed/absent/exception]
     *
     *   Fragments awaiting init completion:
     *     id=[id], node=[nodeName]"
     *     ...
     *
     *   Local fragments:
     *     id=[id], state=[opened/closed], canceled=[true/false], class=[SimpleClassName]  [(root)]
     *     ...
     *
     * ...
     * </pre>
     *
     * @return String containing debugging information.
     */
    String dumpDebugInfo() {
        IgniteStringBuilder buf = new IgniteStringBuilder();

        for (Map.Entry<ExecutionId, DistributedQueryManager> entry : queryManagerMap.entrySet()) {
            ExecutionId executionId = entry.getKey();
            DistributedQueryManager mgr = entry.getValue();

            buf.nl();
            buf.app("Debug info for query: ").app(executionId)
                    .app(" (canceled=").app(mgr.cancelled.get()).app(", stopped=").app(mgr.cancelFut.isDone()).app(")");
            buf.nl();

            buf.app("  Coordinator node: ").app(mgr.coordinatorNodeName);
            if (mgr.coordinator) {
                buf.app(" (current node)");
            }
            buf.nl();

            CompletableFuture<AsyncRootNode<RowT, InternalSqlRow>> rootNodeFut = mgr.root;
            if (rootNodeFut != null) {
                buf.app("  Root node state: ");
                try {
                    AsyncRootNode<RowT, InternalSqlRow> rootNode = rootNodeFut.getNow(null);
                    if (rootNode != null) {
                        if (rootNode.isClosed()) {
                            buf.app("closed");
                        } else {
                            buf.app("opened");
                        }
                    } else {
                        buf.app("absent");
                    }
                } catch (CompletionException ex) {
                    buf.app("completed exceptionally ").app('(').app(ExceptionUtils.unwrapCause(ex)).app(')');
                } catch (CancellationException ex) {
                    buf.app("canceled");
                }
                buf.nl();
            }

            buf.nl();

            List<RemoteFragmentKey> initFragments = mgr.remoteFragmentInitCompletion.entrySet().stream()
                    .filter(entry0 -> !entry0.getValue().isDone())
                    .map(Entry::getKey)
                    .sorted(Comparator.comparingLong(RemoteFragmentKey::fragmentId))
                    .collect(Collectors.toList());

            if (!initFragments.isEmpty()) {
                buf.app("  Fragments awaiting init completion:").nl();

                for (RemoteFragmentKey fragmentKey : initFragments) {
                    buf.app("    id=").app(fragmentKey.fragmentId()).app(", node=").app(fragmentKey.nodeName());
                    buf.nl();
                }

                buf.nl();
            }

            List<AbstractNode<?>> localFragments = mgr.localFragments().stream()
                    .sorted(Comparator.comparingLong(n -> n.context().fragmentId()))
                    .collect(Collectors.toList());

            if (!localFragments.isEmpty()) {
                buf.app("  Local fragments:").nl();

                for (AbstractNode<?> fragment : localFragments) {
                    long fragmentId = fragment.context().fragmentId();

                    buf.app("    id=").app(fragmentId)
                            .app(", state=").app(fragment.isClosed() ? "closed" : "opened")
                            .app(", canceled=").app(fragment.context().isCancelled())
                            .app(", class=").app(fragment.getClass().getSimpleName());

                    Long rootFragmentId = mgr.rootFragmentId;

                    if (rootFragmentId != null && rootFragmentId == fragmentId) {
                        buf.app("  (root)");
                    }

                    buf.nl();
                }
            }
        }

        return buf.length() > 0 ? buf.toString() : " No debug information available.";
    }

    private static String dumpThreads() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] infos = bean.dumpAllThreads(true, true);
        IgniteStringBuilder buf = new IgniteStringBuilder();

        buf.nl().nl().app("Dumping threads:").nl().nl();

        for (ThreadInfo info : infos) {
            buf.app(info.toString()).nl();
        }

        return buf.toString();
    }

    /**
     * A convenient class that manages the initialization and termination of distributed queries.
     */
    private class DistributedQueryManager {
        private final ExecutionId executionId;
        private final boolean coordinator;

        private final String coordinatorNodeName;

        private final SqlOperationContext ctx;

        private final CompletableFuture<Void> cancelFut = new CompletableFuture<>();
        private final PrefetchCallback prefetchCallback = new PrefetchCallback();
        private final AtomicBoolean cancelled = new AtomicBoolean();

        private final Map<RemoteFragmentKey, CompletableFuture<Void>> remoteFragmentInitCompletion = new HashMap<>();

        private final Queue<AbstractNode<RowT>> localFragments = new ConcurrentLinkedQueue<>();

        private final @Nullable CompletableFuture<AsyncRootNode<RowT, InternalSqlRow>> root;

        private final @Nullable QueryCancel cancel;

        /** Mutex for {@link #remoteFragmentInitCompletion} modifications. */
        private final Object initMux = new Object();

        private volatile Long rootFragmentId = null;

        private DistributedQueryManager(
                ExecutionId executionId,
                String coordinatorNodeName,
                boolean coordinator,
                SqlOperationContext ctx
        ) {
            this.executionId = executionId;
            this.ctx = ctx;
            this.coordinator = coordinator;
            this.coordinatorNodeName = coordinatorNodeName;

            if (coordinator) {
                var root = new CompletableFuture<AsyncRootNode<RowT, InternalSqlRow>>();

                root.exceptionally(t -> {
                    this.close(CancellationReason.CANCEL);

                    return null;
                });

                this.root = root;
                this.cancel = ctx.cancel();
            } else {
                this.root = null;
                this.cancel = null;
            }
        }

        private DistributedQueryManager(ExecutionId executionId, String coordinatorNodeName, SqlOperationContext ctx) {
            this(executionId, coordinatorNodeName, false, ctx);
        }

        private List<AbstractNode<?>> localFragments() {
            return List.copyOf(localFragments);
        }

        private CompletableFuture<Void> sendFragment(
                String targetNodeName, String serialisedFragment, FragmentDescription desc, TxAttributes txAttributes, int catalogVersion
        ) {
            QueryStartRequest request = FACTORY.queryStartRequest()
                    .queryId(executionId.queryId())
                    .executionToken(executionId.executionToken())
                    .fragmentId(desc.fragmentId())
                    .root(serialisedFragment)
                    .fragmentDescription(desc)
                    .parameters(ctx.parameters())
                    .txAttributes(txAttributes)
                    .catalogVersion(catalogVersion)
                    .timeZoneId(ctx.timeZoneId().getId())
                    .operationTime(ctx.operationTime())
                    .timestamp(clockService.now())
                    .build();

            return messageService.send(targetNodeName, request);
        }

        private void acknowledgeFragment(String nodeName, long fragmentId, @Nullable Throwable ex) {
            if (ex != null) {
                Long rootFragmentId0 = rootFragmentId;

                if (rootFragmentId0 != null && fragmentId == rootFragmentId0) {
                    root.completeExceptionally(ex);
                } else {
                    root.thenAccept(root -> {
                        root.onError(ex);

                        close(CancellationReason.CANCEL);
                    });
                }
            }

            remoteFragmentInitCompletion.get(new RemoteFragmentKey(nodeName, fragmentId)).complete(null);
        }

        private void onError(RemoteFragmentExecutionException ex) {
            root.thenAccept(root -> {
                root.onError(ex);

                close(CancellationReason.CANCEL);
            });
        }

        private void onNodeLeft(String nodeName) {
            remoteFragmentInitCompletion.entrySet().stream()
                    .filter(e -> nodeName.equals(e.getKey().nodeName()))
                    .forEach(e -> e.getValue().completeExceptionally(new NodeLeftException(nodeName)));
        }

        private CompletableFuture<Void> executeFragment(IgniteRel treeRoot, ResolvedDependencies deps, ExecutionContext<RowT> ectx) {
            String origNodeName = ectx.originatingNodeName();

            AbstractNode<RowT> node = implementorFactory.create(ectx, deps).go(treeRoot);

            localFragments.add(node);

            if (!(node instanceof Outbox)) {
                BiFunction<Integer, Object, Object> internalTypeConverter = TypeUtils.resultTypeConverter(ectx, treeRoot.getRowType());

                AsyncRootNode<RowT, InternalSqlRow> rootNode = new AsyncRootNode<>(
                        node,
                        inRow -> new InternalSqlRowImpl<>(inRow, ectx.rowHandler(), internalTypeConverter));
                node.onRegister(rootNode);

                rootNode.startPrefetch()
                        .whenCompleteAsync((res, err) -> prefetchCallback.onPrefetchComplete(err), taskExecutor);

                assert root != null;

                root.complete(rootNode);
            }

            CompletableFuture<Void> sendingResult = messageService.send(
                    origNodeName,
                    FACTORY.queryStartResponse()
                            .queryId(ectx.queryId())
                            .executionToken(ectx.executionToken())
                            .fragmentId(ectx.fragmentId())
                            .build()
            );

            if (node instanceof Outbox) {
                ((Outbox<?>) node).prefetch();
            }

            return sendingResult;
        }

        private ExecutionContext<RowT> createContext(String initiatorNodeName, FragmentDescription desc, TxAttributes txAttributes) {
            return new ExecutionContext<>(
                    taskExecutor,
                    executionId,
                    localNode,
                    initiatorNodeName,
                    desc,
                    handler,
                    Commons.parametersMap(ctx.parameters()),
                    txAttributes,
                    ctx.timeZoneId(),
                    cancel
            );
        }

        private void submitFragment(
                String initiatorNode,
                int catalogVersion,
                String fragmentString,
                FragmentDescription desc,
                TxAttributes txAttributes
        ) {
            try {
                ExecutionContext<RowT> context = createContext(initiatorNode, desc, txAttributes);
                IgniteRel treeRoot = relationalTreeFromJsonString(catalogVersion, fragmentString);

                ResolvedDependencies resolvedDependencies = dependencyResolver.resolveDependencies(List.of(treeRoot), catalogVersion);
                executeFragment(treeRoot, resolvedDependencies, context)
                        .exceptionally(ex -> {
                            handleError(ex, initiatorNode, desc.fragmentId());

                            return null;
                        });
            } catch (Throwable ex) {
                handleError(ex, initiatorNode, desc.fragmentId());
            }
        }

        private void handleError(Throwable ex, String initiatorNode, long fragmentId) {
            LOG.debug("Unable to start query fragment", ex);

            try {
                messageService.send(
                        initiatorNode,
                        FACTORY.queryStartResponse()
                                .queryId(executionId.queryId())
                                .executionToken(executionId.executionToken())
                                .fragmentId(fragmentId)
                                .error(ex)
                                .build()
                );
            } catch (Exception e) {
                LOG.info("Unable to send error message", e);

                close(CancellationReason.CANCEL);
            }
        }

        private CompletableFuture<AsyncCursor<InternalSqlRow>> execute(
                InternalTransaction tx,
                MultiStepPlan multiStepPlan,
                @Nullable Predicate<String> nodeExclusionFilter
        ) {
            assert root != null;

            boolean mapOnBackups = tx.isReadOnly();
            MappingParameters mappingParameters = MappingParameters.create(ctx.parameters(), mapOnBackups, nodeExclusionFilter);

            return mappingService.map(multiStepPlan, mappingParameters)
                    .thenComposeAsync(mappedFragments -> sendFragments(tx, multiStepPlan, mappedFragments), taskExecutor)
                    .thenApply(this::wrapRootNode);
        }

        private CompletableFuture<AsyncCursor<InternalSqlRow>> sendFragments(
                InternalTransaction tx,
                MultiStepPlan multiStepPlan,
                List<MappedFragment> mappedFragments
        ) {
            // we rely on the fact that the very first fragment is a root. Otherwise we need to handle
            // the case when a non-root fragment will fail before the root is processed.
            assert !nullOrEmpty(mappedFragments) && mappedFragments.get(0).fragment().rootFragment()
                    : mappedFragments;

            // first let's enlist all tables to the transaction.
            if (!tx.isReadOnly()) {
                for (MappedFragment mappedFragment : mappedFragments) {
                    enlistPartitions(mappedFragment, tx);
                }
            }

            synchronized (initMux) {
                QueryCancel queryCancel = ctx.cancel();

                // skipping initialization if cancel has already been triggered
                if (queryCancel != null) {
                    queryCancel.throwIfCancelled();
                }

                // then let's register all remote fragment's initialization futures. This need
                // to handle race when root fragment initialization failed and the query is cancelled
                // while sending is actually still in progress
                for (MappedFragment mappedFragment : mappedFragments) {
                    if (mappedFragment.fragment().rootFragment()) {
                        assert rootFragmentId == null;

                        rootFragmentId = mappedFragment.fragment().fragmentId();
                    }

                    for (String nodeName : mappedFragment.nodes()) {
                        remoteFragmentInitCompletion.put(
                                new RemoteFragmentKey(nodeName, mappedFragment.fragment().fragmentId()),
                                new CompletableFuture<>()
                        );
                    }
                }
            }

            // now transaction is initialized for sure including assignment
            // of the commit partition (if there is at least one table in the fragments)
            TxAttributes attributes = TxAttributes.fromTx(tx);

            List<CompletableFuture<?>> resultsOfFragmentSending = new ArrayList<>();

            // start remote execution
            for (MappedFragment mappedFragment : mappedFragments) {
                Fragment fragment = mappedFragment.fragment();

                FragmentDescription fragmentDesc = new FragmentDescription(
                        fragment.fragmentId(),
                        !fragment.correlated(),
                        mappedFragment.groupsBySourceId(),
                        mappedFragment.target(),
                        mappedFragment.sourcesByExchangeId(),
                        mappedFragment.partitionPruningMetadata()
                );

                for (String nodeName : mappedFragment.nodes()) {
                    CompletableFuture<Void> resultOfSending =
                            sendFragment(nodeName, fragment.serialized(), fragmentDesc, attributes, multiStepPlan.catalogVersion());

                    resultOfSending.whenComplete((ignored, t) -> {
                        if (t == null) {
                            return;
                        }

                        // if we were unable to send a request, then no need
                        // to wait for the remote node to complete initialization

                        CompletableFuture<?> completionFuture = remoteFragmentInitCompletion.get(
                                new RemoteFragmentKey(nodeName, fragment.fragmentId())
                        );

                        if (completionFuture != null) {
                            completionFuture.complete(null);
                        }
                    });

                    resultsOfFragmentSending.add(resultOfSending);
                }
            }

            return CompletableFutures.allOf(resultsOfFragmentSending)
                    .handle((ignoredVal, ignoredTh) -> {
                        if (ignoredTh == null) {
                            return root;
                        }

                        Throwable error = Commons.deriveExceptionFromListOfFutures(resultsOfFragmentSending);

                        assert error != null;

                        return CompletableFutures.allOf(remoteFragmentInitCompletion.values())
                                .thenCompose(none -> {
                                    if (!root.completeExceptionally(error)) {
                                        root.thenAccept(root -> root.onError(error));

                                        close(CancellationReason.CANCEL);
                                    }

                                    return cancelFut
                                            .thenRun(() -> sneakyThrow(error));
                                })
                                .thenCompose(none -> root);
                    })
                    .thenCompose(Commons::cast);
        }

        private void enlistPartitions(MappedFragment mappedFragment, InternalTransaction tx) {
            // no need to traverse the tree if fragment has no tables
            if (mappedFragment.fragment().tables().isEmpty()) {
                return;
            }

            new IgniteRelShuttle() {
                @Override
                public IgniteRel visit(IgniteIndexScan rel) {
                    enlist(rel);

                    return super.visit(rel);
                }

                @Override
                public IgniteRel visit(IgniteTableScan rel) {
                    enlist(rel);

                    return super.visit(rel);
                }

                @Override
                public IgniteRel visit(IgniteTableModify rel) {
                    enlist(rel);

                    return super.visit(rel);
                }

                private void enlist(int tableId, Int2ObjectMap<NodeWithConsistencyToken> assignments) {
                    if (assignments.isEmpty()) {
                        return;
                    }

                    int partsCnt = assignments.size();

                    tx.assignCommitPartition(new TablePartitionId(tableId, ThreadLocalRandom.current().nextInt(partsCnt)));

                    for (Map.Entry<Integer, NodeWithConsistencyToken> partWithToken : assignments.int2ObjectEntrySet()) {
                        TablePartitionId tablePartId = new TablePartitionId(tableId, partWithToken.getKey());

                        NodeWithConsistencyToken assignment = partWithToken.getValue();

                        tx.enlist(tablePartId,
                                new IgniteBiTuple<>(
                                        topSrvc.getByConsistentId(assignment.name()),
                                        assignment.enlistmentConsistencyToken())
                        );
                    }
                }

                private void enlist(SourceAwareIgniteRel rel) {
                    int tableId = rel.getTable().unwrap(IgniteTable.class).id();

                    ColocationGroup colocationGroup = mappedFragment.groupsBySourceId().get(rel.sourceId());
                    Int2ObjectMap<NodeWithConsistencyToken> assignments = colocationGroup.assignments();

                    enlist(tableId, assignments);
                }
            }.visit(mappedFragment.fragment().root());
        }

        private CompletableFuture<Void> close(CancellationReason reason) {
            if (!cancelled.compareAndSet(false, true)) {
                return cancelFut;
            }

            CompletableFuture<Void> start = new CompletableFuture<>();

            CompletableFuture<Void> stage;

            if (coordinator) {
                stage = start.thenCompose(ignored -> closeRootNode(reason))
                        .thenCompose(ignored -> awaitFragmentInitialisationAndClose());
            } else {
                stage = start.thenCompose(ignored -> messageService.send(coordinatorNodeName, FACTORY.queryCloseMessage()
                                .queryId(executionId.queryId())
                                .executionToken(executionId.executionToken())
                                .build()))
                        .thenCompose(ignored -> closeLocalFragments());
            }

            stage.whenComplete((r, e) -> {
                if (e != null) {
                    Throwable ex = ExceptionUtils.unwrapCause(e);

                    LOG.warn("Fragment closing processed with errors: [queryId={}]", ex, ctx.queryId());
                }

                queryManagerMap.remove(executionId);

                cancelFut.complete(null);
            }).thenRun(() -> localFragments.forEach(f -> f.context().cancel()));

            start.completeAsync(() -> null, taskExecutor);

            return cancelFut;
        }

        private CompletableFuture<Void> closeLocalFragments() {
            List<CompletableFuture<?>> localFragmentCompletions = new ArrayList<>();
            for (AbstractNode<?> node : localFragments) {
                assert !node.context().isCancelled() : "node context is cancelled, but node still processed";

                localFragmentCompletions.add(
                        node.context().submit(node::close, node::onError)
                );
            }

            return CompletableFuture.allOf(
                    localFragmentCompletions.toArray(new CompletableFuture[0])
            );
        }

        private CompletableFuture<Void> awaitFragmentInitialisationAndClose() {
            Map<String, List<CompletableFuture<?>>> requestsPerNode = new HashMap<>();

            synchronized (initMux) {
                for (Map.Entry<RemoteFragmentKey, CompletableFuture<Void>> entry : remoteFragmentInitCompletion.entrySet()) {
                    requestsPerNode.computeIfAbsent(entry.getKey().nodeName(), key -> new ArrayList<>()).add(entry.getValue());
                }
            }

            List<CompletableFuture<?>> cancelFuts = new ArrayList<>();
            for (Map.Entry<String, List<CompletableFuture<?>>> entry : requestsPerNode.entrySet()) {
                String nodeId = entry.getKey();

                cancelFuts.add(
                        CompletableFuture.allOf(entry.getValue().toArray(new CompletableFuture[0]))
                                .handle((none2, t) -> {
                                    // Some fragments may be completed exceptionally, and that is fine.
                                    // Here we need to make sure that all query initialisation requests
                                    // have been processed before sending a cancellation request. That's
                                    // why we just ignore any exception.

                                    return null;
                                })
                                .thenCompose(ignored -> {
                                    // for local fragments don't send the message, just close the fragments
                                    if (localNode.name().equals(nodeId)) {
                                        return closeLocalFragments();
                                    }

                                    return messageService.send(
                                            nodeId,
                                            FACTORY.queryCloseMessage()
                                                    .queryId(executionId.queryId())
                                                    .executionToken(executionId.executionToken())
                                                    .build()
                                    );
                                })
                );
            }

            return CompletableFuture.allOf(cancelFuts.toArray(new CompletableFuture[0]));
        }

        /**
         * Synchronously closes the tree's execution iterator.
         *
         * @param closeReason Reason to use in {@link QueryCancelledException}.
         * @return Completable future that should run asynchronously.
         */
        private CompletableFuture<Void> closeRootNode(CancellationReason closeReason) {
            assert root != null;

            String message;
            if (closeReason == CancellationReason.TIMEOUT) {
                message = QueryCancelledException.TIMEOUT_MSG;
            } else {
                message = QueryCancelledException.CANCEL_MSG;
            }

            if (!root.isDone()) {
                root.completeExceptionally(new QueryCancelledException(message));
            }

            if (!root.isCompletedExceptionally()) {
                AsyncRootNode<RowT, InternalSqlRow> node = root.getNow(null);

                if (closeReason != CancellationReason.CLOSE) {
                    node.onError(new QueryCancelledException(message));
                }

                return node.closeAsync();
            }

            return nullCompletedFuture();
        }

        private AsyncCursor<InternalSqlRow> wrapRootNode(AsyncCursor<InternalSqlRow> cursor) {
            return new AsyncCursor<>() {
                @Override
                public CompletableFuture<BatchedResult<InternalSqlRow>> requestNextAsync(int rows) {
                    CompletableFuture<BatchedResult<InternalSqlRow>> fut = cursor.requestNextAsync(rows);

                    fut.thenAccept(batch -> {
                        if (!batch.hasMore()) {
                            DistributedQueryManager.this.close(CancellationReason.CLOSE);
                        }
                    });

                    return fut;
                }

                @Override
                public CompletableFuture<Void> closeAsync() {
                    return DistributedQueryManager.this.close(CancellationReason.CLOSE);
                }
            };
        }
    }

    private ExecutionId nextExecutionId(UUID queryId) {
        return new ExecutionId(queryId, executionTokenGen.getAndIncrement());
    }

    /**
     * A factory of the relational node implementors.
     *
     * @param <RowT> A type of the row the execution tree will be working with.
     * @see LogicalRelImplementor
     */
    @FunctionalInterface
    public interface ImplementorFactory<RowT> {
        /** Creates the relational node implementor with the given context. */
        LogicalRelImplementor<RowT> create(ExecutionContext<RowT> ctx, ResolvedDependencies resolvedDependencies);
    }

    private static class FragmentCacheKey {
        private final int catalogVersion;
        private final String fragmentString;

        FragmentCacheKey(int catalogVersion, String fragmentString) {
            this.catalogVersion = catalogVersion;
            this.fragmentString = fragmentString;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FragmentCacheKey that = (FragmentCacheKey) o;

            return catalogVersion == that.catalogVersion
                    && fragmentString.equals(that.fragmentString);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogVersion, fragmentString);
        }
    }
}
