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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.externalize.RelJsonReader.fromJson;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
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
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.InternalSqlRowImpl;
import org.apache.ignite.internal.sql.engine.InternalSqlRowSingleBoolean;
import org.apache.ignite.internal.sql.engine.InternalSqlRowSingleString;
import org.apache.ignite.internal.sql.engine.NodeLeftException;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.QueryPrefetchCallback;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionRegistry;
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
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.SourceAwareIgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.AsyncWrapper;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;

/**
 * Provide ability to execute SQL query plan and retrieve results of the execution.
 */
public class ExecutionServiceImpl<RowT> implements ExecutionService, TopologyEventHandler {
    private static final int CACHE_SIZE = 1024;

    private final ConcurrentMap<FragmentCacheKey, IgniteRel> physNodesCache = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .<FragmentCacheKey, IgniteRel>build()
            .asMap();

    private static final IgniteLogger LOG = Loggers.forClass(ExecutionServiceImpl.class);

    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    private static final List<InternalSqlRow> APPLIED_ANSWER = List.of(new InternalSqlRowSingleBoolean(true));

    private static final List<InternalSqlRow> NOT_APPLIED_ANSWER = List.of(new InternalSqlRowSingleBoolean(false));

    private static final FragmentDescription DUMMY_DESCRIPTION = new FragmentDescription(
            0, true, Long2ObjectMaps.emptyMap(), null, null, null
    );

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

    private final Map<UUID, DistributedQueryManager> queryManagerMap = new ConcurrentHashMap<>();

    private final long shutdownTimeout;

    private final ClockService clockService;

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

    private AsyncCursor<InternalSqlRow> executeQuery(
            InternalTransaction tx,
            BaseQueryContext ctx,
            MultiStepPlan plan
    ) {
        DistributedQueryManager queryManager = new DistributedQueryManager(localNode.name(), true, ctx);

        DistributedQueryManager old = queryManagerMap.put(ctx.queryId(), queryManager);

        assert old == null;

        ctx.cancel().add(() -> queryManager.close(true));

        return queryManager.execute(tx, plan);
    }

    private BaseQueryContext createQueryContext(UUID queryId, int schemaVersion, ZoneId timeZoneId, Object[] params) {
        return BaseQueryContext.builder()
                .queryId(queryId)
                .parameters(params)
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(sqlSchemaManager.schema(schemaVersion))
                                .build()
                )
                .timeZoneId(timeZoneId)
                .build();
    }

    private IgniteRel relationalTreeFromJsonString(int schemaVersion, String jsonFragment, BaseQueryContext ctx) {
        return physNodesCache.computeIfAbsent(
                new FragmentCacheKey(schemaVersion, jsonFragment),
                key -> fromJson(ctx, key.fragmentString)
        );
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("CastConflictsWithInstanceof") // IDEA incorrectly highlights casts in EXPLAIN and DDL branches
    public AsyncCursor<InternalSqlRow> executePlan(
            InternalTransaction tx, QueryPlan plan, BaseQueryContext ctx
    ) {
        SqlQueryType queryType = plan.type();

        switch (queryType) {
            case DML:
            case QUERY:
                if (plan instanceof ExecutablePlan) {
                    return executeExecutablePlan(tx, ctx, (ExecutablePlan) plan, ctx.prefetchCallback());
                }

                assert plan instanceof MultiStepPlan : plan.getClass();

                return executeQuery(tx, ctx, (MultiStepPlan) plan);
            case EXPLAIN:
                return executeExplain((ExplainPlan) plan, ctx.prefetchCallback());
            case DDL:
                return executeDdl((DdlPlan) plan, ctx.prefetchCallback());

            default:
                throw new AssertionError("Unexpected query type: " + plan);
        }
    }

    /** Cancels the query with given id. */
    public CompletableFuture<?> cancel(UUID qryId) {
        var mgr = queryManagerMap.get(qryId);

        if (mgr == null) {
            return nullCompletedFuture();
        }

        return mgr.close(true);
    }

    private AsyncCursor<InternalSqlRow> executeExecutablePlan(
            InternalTransaction tx,
            BaseQueryContext ctx,
            ExecutablePlan plan,
            @Nullable QueryPrefetchCallback callback
    ) {
        ExecutionContext<RowT> ectx = new ExecutionContext<>(
                taskExecutor,
                ctx.queryId(),
                localNode,
                localNode.name(),
                DUMMY_DESCRIPTION,
                handler,
                Commons.parametersMap(ctx.parameters()),
                TxAttributes.fromTx(tx),
                ctx.timeZoneId()
        );

        return plan.execute(ectx, tx, tableRegistry, callback);
    }

    private AsyncCursor<InternalSqlRow> executeDdl(DdlPlan plan, @Nullable QueryPrefetchCallback callback) {
        CompletableFuture<Iterator<InternalSqlRow>> ret = ddlCmdHnd.handle(plan.command())
                .thenApply(applied -> (applied ? APPLIED_ANSWER : NOT_APPLIED_ANSWER).iterator())
                .exceptionally(th -> {
                    throw convertDdlException(th);
                });

        if (callback != null) {
            ret.whenCompleteAsync((res, err) -> callback.onPrefetchComplete(err), taskExecutor);
        }

        return new AsyncWrapper<>(ret, Runnable::run);
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

    private AsyncCursor<InternalSqlRow> executeExplain(ExplainPlan plan, @Nullable QueryPrefetchCallback callback) {
        String planString = plan.plan().explain();

        InternalSqlRow res = new InternalSqlRowSingleString(planString);

        if (callback != null) {
            taskExecutor.execute(() -> callback.onPrefetchComplete(null));
        }

        return new AsyncWrapper<>(List.of(res).iterator());
    }

    private void onMessage(String nodeName, QueryStartRequest msg) {
        assert nodeName != null && msg != null;

        CompletableFuture<Void> fut = sqlSchemaManager.schemaReadyFuture(msg.schemaVersion());

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

        DistributedQueryManager dqm = queryManagerMap.get(msg.queryId());

        if (dqm != null) {
            dqm.acknowledgeFragment(nodeName, msg.fragmentId(), msg.error());
        }
    }

    private void onMessage(String nodeName, ErrorMessage msg) {
        assert nodeName != null && msg != null;

        DistributedQueryManager dqm = queryManagerMap.get(msg.queryId());

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

        DistributedQueryManager dqm = queryManagerMap.get(msg.queryId());

        if (dqm != null) {
            dqm.close(true);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        CompletableFuture<Void> f = CompletableFuture.allOf(queryManagerMap.values().stream()
                .filter(mgr -> mgr.rootFragmentId != null)
                .map(mgr -> mgr.close(true))
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
    public List<AbstractNode<?>> localFragments(UUID queryId) {
        DistributedQueryManager mgr = queryManagerMap.get(queryId);

        if (mgr == null) {
            return List.of();
        }

        return mgr.localFragments();
    }

    private void submitFragment(String nodeName, QueryStartRequest msg) {
        DistributedQueryManager queryManager = getOrCreateQueryManager(nodeName, msg);

        queryManager.submitFragment(nodeName, msg.schemaVersion(), msg.root(), msg.fragmentDescription(), msg.txAttributes());
    }

    private void handleError(Throwable ex, String nodeName, QueryStartRequest msg) {
        DistributedQueryManager queryManager = getOrCreateQueryManager(nodeName, msg);

        queryManager.handleError(ex, nodeName, msg.fragmentDescription().fragmentId());
    }

    private DistributedQueryManager getOrCreateQueryManager(String coordinatorNodeName, QueryStartRequest msg) {
        return queryManagerMap.computeIfAbsent(msg.queryId(), key -> {
            BaseQueryContext ctx = createQueryContext(key, msg.schemaVersion(), ZoneId.of(msg.timeZoneId()), msg.parameters());

            return new DistributedQueryManager(coordinatorNodeName, ctx);
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

        for (Map.Entry<UUID, DistributedQueryManager> entry : queryManagerMap.entrySet()) {
            UUID queryId = entry.getKey();
            DistributedQueryManager mgr = entry.getValue();

            buf.nl();
            buf.app("Debug info for query: ").app(queryId)
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
        private final boolean coordinator;

        private final String coordinatorNodeName;

        private final BaseQueryContext ctx;

        private final CompletableFuture<Void> cancelFut = new CompletableFuture<>();

        private final AtomicBoolean cancelled = new AtomicBoolean();

        private final Map<RemoteFragmentKey, CompletableFuture<Void>> remoteFragmentInitCompletion = new ConcurrentHashMap<>();

        private final Queue<AbstractNode<RowT>> localFragments = new ConcurrentLinkedQueue<>();

        private final @Nullable CompletableFuture<AsyncRootNode<RowT, InternalSqlRow>> root;

        private volatile Long rootFragmentId = null;

        private DistributedQueryManager(
                String coordinatorNodeName,
                boolean coordinator,
                BaseQueryContext ctx
        ) {
            this.ctx = ctx;
            this.coordinator = coordinator;
            this.coordinatorNodeName = coordinatorNodeName;

            if (coordinator) {
                var root = new CompletableFuture<AsyncRootNode<RowT, InternalSqlRow>>();

                root.exceptionally(t -> {
                    this.close(true);

                    QueryPrefetchCallback callback = ctx.prefetchCallback();

                    if (callback != null) {
                        taskExecutor.execute(() -> callback.onPrefetchComplete(t));
                    }

                    return null;
                });

                this.root = root;
            } else {
                this.root = null;
            }
        }

        private DistributedQueryManager(String coordinatorNodeName, BaseQueryContext ctx) {
            this(coordinatorNodeName, false, ctx);
        }

        private List<AbstractNode<?>> localFragments() {
            return List.copyOf(localFragments);
        }

        private CompletableFuture<Void> sendFragment(
                String targetNodeName, String serialisedFragment, FragmentDescription desc, TxAttributes txAttributes
        ) {
            QueryStartRequest request = FACTORY.queryStartRequest()
                    .queryId(ctx.queryId())
                    .fragmentId(desc.fragmentId())
                    .root(serialisedFragment)
                    .fragmentDescription(desc)
                    .parameters(ctx.parameters())
                    .txAttributes(txAttributes)
                    .schemaVersion(ctx.schemaVersion())
                    .timeZoneId(ctx.timeZoneId().getId())
                    .timestampLong(clockService.nowLong())
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

                        close(true);
                    });
                }
            }

            remoteFragmentInitCompletion.get(new RemoteFragmentKey(nodeName, fragmentId)).complete(null);
        }

        private void onError(RemoteFragmentExecutionException ex) {
            root.thenAccept(root -> {
                root.onError(ex);

                close(true);
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

                CompletableFuture<Void> prefetchFut = rootNode.startPrefetch();
                QueryPrefetchCallback callback = ctx.prefetchCallback();

                if (callback != null) {
                    prefetchFut.whenCompleteAsync((res, err) -> callback.onPrefetchComplete(err), taskExecutor);
                }

                root.complete(rootNode);
            }

            CompletableFuture<Void> sendingResult = messageService.send(
                    origNodeName,
                    FACTORY.queryStartResponse()
                            .queryId(ectx.queryId())
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
                    ctx.queryId(),
                    localNode,
                    initiatorNodeName,
                    desc,
                    handler,
                    Commons.parametersMap(ctx.parameters()),
                    txAttributes,
                    ctx.timeZoneId()
            );
        }

        private void submitFragment(
                String initiatorNode,
                int schemaVersion,
                String fragmentString,
                FragmentDescription desc,
                TxAttributes txAttributes
        ) {
            try {
                // Because fragment execution runs on specific thread selected by taskExecutor,
                // we should complete dependency resolution on the same thread
                // that is going to be used for fragment execution.
                ExecutionContext<RowT> context = createContext(initiatorNode, desc, txAttributes);
                Executor exec = (r) -> context.execute(r::run, err -> handleError(err, initiatorNode, desc.fragmentId()));
                IgniteRel treeRoot = relationalTreeFromJsonString(schemaVersion, fragmentString, ctx);

                dependencyResolver.resolveDependencies(List.of(treeRoot), schemaVersion)
                        .thenComposeAsync(deps -> executeFragment(treeRoot, deps, context), exec)
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
                                .queryId(ctx.queryId())
                                .fragmentId(fragmentId)
                                .error(ex)
                                .build()
                );
            } catch (Exception e) {
                LOG.info("Unable to send error message", e);

                close(true);
            }
        }

        private AsyncCursor<InternalSqlRow> execute(InternalTransaction tx, MultiStepPlan multiStepPlan) {
            assert root != null;

            MappingParameters mappingParameters = MappingParameters.create(ctx.parameters());

            mappingService.map(multiStepPlan, mappingParameters).whenCompleteAsync((mappedFragments, mappingErr) -> {
                if (mappingErr != null) {
                    if (!root.completeExceptionally(mappingErr)) {
                        root.thenAccept(root -> root.onError(mappingErr));
                    }
                    return;
                }

                try {
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

                    // then let's register all remote fragment's initialization futures. This need
                    // to handle race when root fragment initialization failed and the query is cancelled
                    // while sending is actually still in progress
                    for (MappedFragment mappedFragment : mappedFragments) {
                        for (String nodeName : mappedFragment.nodes()) {
                            remoteFragmentInitCompletion.put(
                                    new RemoteFragmentKey(nodeName, mappedFragment.fragment().fragmentId()),
                                    new CompletableFuture<>()
                            );
                        }
                    }

                    // now transaction is initialized for sure including assignment
                    // of the commit partition (if there is at least one table in the fragments)
                    TxAttributes attributes = TxAttributes.fromTx(tx);

                    List<CompletableFuture<?>> resultsOfFragmentSending = new ArrayList<>();

                    // start remote execution
                    for (MappedFragment mappedFragment : mappedFragments) {
                        Fragment fragment = mappedFragment.fragment();

                        if (fragment.rootFragment()) {
                            assert rootFragmentId == null;

                            rootFragmentId = fragment.fragmentId();
                        }

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
                                    sendFragment(nodeName, fragment.serialized(), fragmentDesc, attributes);

                            resultsOfFragmentSending.add(
                                    resultOfSending.handle((ignored, t) -> {
                                        if (t == null) {
                                            return null;
                                        }

                                        // if we were unable to send a request, then no need
                                        // to wait for the remote node to complete initialization

                                        CompletableFuture<?> completionFuture = remoteFragmentInitCompletion.get(
                                                new RemoteFragmentKey(nodeName, fragment.fragmentId())
                                        );

                                        if (completionFuture != null) {
                                            completionFuture.complete(null);
                                        }

                                        throw ExceptionUtils.withCause(
                                                t instanceof NodeLeftException ? NodeLeftException::new : IgniteInternalException::new,
                                                INTERNAL_ERR,
                                                format("Unable to send fragment [targetNode={}, fragmentId={}, cause={}]",
                                                        nodeName, fragment.fragmentId(), t.getMessage()), t
                                        );
                                    })
                            );
                        }
                    }

                    CompletableFuture.allOf(resultsOfFragmentSending.toArray(new CompletableFuture[0]))
                            .handle((ignoredVal, ignoredTh) -> {
                                if (ignoredTh == null) {
                                    return null;
                                }

                                Throwable firstFoundError = null;

                                for (CompletableFuture<?> fut : resultsOfFragmentSending) {
                                    if (fut.isCompletedExceptionally()) {
                                        // this is non blocking join() because we are inside of CompletableFuture.allOf call
                                        Throwable fromFuture = fut.handle((ignored, ex) -> ex).join();

                                        if (firstFoundError == null) {
                                            firstFoundError = fromFuture;
                                        } else {
                                            firstFoundError.addSuppressed(fromFuture);
                                        }
                                    }
                                }

                                Throwable error = firstFoundError;
                                if (!root.completeExceptionally(error)) {
                                    root.thenAccept(root -> root.onError(error));
                                }

                                return null;
                            });
                } catch (Throwable t) {
                    LOG.warn("Unexpected exception during query initialization", t);

                    if (!root.completeExceptionally(t)) {
                        root.thenAccept(root -> root.onError(t));
                    }
                }
            }, taskExecutor);

            return new AsyncCursor<>() {
                @Override
                public CompletableFuture<BatchedResult<InternalSqlRow>> requestNextAsync(int rows) {
                    return root.thenCompose(cur -> {
                        CompletableFuture<BatchedResult<InternalSqlRow>> fut = cur.requestNextAsync(rows);

                        fut.thenAccept(batch -> {
                            if (!batch.hasMore()) {
                                DistributedQueryManager.this.close(false);
                            }
                        });

                        return fut;
                    });
                }

                @Override
                public CompletableFuture<Void> closeAsync() {
                    return root.handle((ignored, ex) -> {
                        if (ex != null) {
                            // cancellation should be triggered by listener of exceptional
                            // completion of `root` future, thus let's just return a result here
                            return DistributedQueryManager.this.cancelFut;
                        }

                        return DistributedQueryManager.this.close(false);
                    }).thenCompose(Function.identity());
                }
            };
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

        private CompletableFuture<Void> close(boolean cancel) {
            if (!cancelled.compareAndSet(false, true)) {
                return cancelFut;
            }

            CompletableFuture<Void> start = new CompletableFuture<>();

            CompletableFuture<Void> stage;

            if (coordinator) {
                stage = start.thenCompose(ignored -> closeRootNode(cancel))
                        .thenCompose(ignored -> awaitFragmentInitialisationAndClose());
            } else {
                stage = start.thenCompose(ignored -> messageService.send(coordinatorNodeName, FACTORY.queryCloseMessage()
                                .queryId(ctx.queryId())
                                .build()))
                        .thenCompose(ignored -> closeLocalFragments());
            }

            stage.whenComplete((r, e) -> {
                if (e != null) {
                    Throwable ex = ExceptionUtils.unwrapCause(e);

                    LOG.warn("Fragment closing processed with errors: [queryId={}]", ex, ctx.queryId());
                }

                queryManagerMap.remove(ctx.queryId());

                try {
                    ctx.cancel().cancel();
                } catch (Exception th) {
                    LOG.debug("Exception raised while cancel", th);
                }

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
            for (Map.Entry<RemoteFragmentKey, CompletableFuture<Void>> entry : remoteFragmentInitCompletion.entrySet()) {
                requestsPerNode.computeIfAbsent(entry.getKey().nodeName(), key -> new ArrayList<>()).add(entry.getValue());
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
                                                    .queryId(ctx.queryId())
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
         * @param cancel Forces execution to terminate with {@link QueryCancelledException}.
         * @return Completable future that should run asynchronously.
         */
        private CompletableFuture<Void> closeRootNode(boolean cancel) {
            assert root != null;

            if (!root.isDone()) {
                root.completeExceptionally(new QueryCancelledException());
            }

            if (!root.isCompletedExceptionally()) {
                AsyncRootNode<RowT, InternalSqlRow> node = root.getNow(null);

                if (cancel) {
                    node.onError(new QueryCancelledException());
                }

                return node.closeAsync();
            }

            return nullCompletedFuture();
        }
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
        private final int schemaVersion;
        private final String fragmentString;

        FragmentCacheKey(int schemaVersion, String fragmentString) {
            this.schemaVersion = schemaVersion;
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

            return schemaVersion == that.schemaVersion
                    && fragmentString.equals(that.fragmentString);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaVersion, fragmentString);
        }
    }
}
