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
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.engine.NodeLeftException;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.QueryPrefetchCallback;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappedFragment;
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
import org.apache.ignite.internal.sql.engine.prepare.DynamicParameterValue;
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
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.HashFunctionFactoryImpl;
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
 * ExecutionServiceImpl. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class ExecutionServiceImpl<RowT> implements ExecutionService, TopologyEventHandler {
    private static final int CACHE_SIZE = 1024;

    private final ConcurrentMap<FragmentCacheKey, IgniteRel> physNodesCache = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .<FragmentCacheKey, IgniteRel>build()
            .asMap();

    private static final IgniteLogger LOG = Loggers.forClass(ExecutionServiceImpl.class);

    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    private final MessageService messageService;

    private final TopologyService topSrvc;

    private final ClusterNode localNode;

    private final SqlSchemaManager sqlSchemaManager;

    private final QueryTaskExecutor taskExecutor;

    private final MappingService mappingService;

    private final RowHandler<RowT> handler;

    private final DdlCommandHandler ddlCmdHnd;

    private final ExecutionDependencyResolver dependencyResolver;

    private final ImplementorFactory<RowT> implementorFactory;

    private final Map<UUID, DistributedQueryManager> queryManagerMap = new ConcurrentHashMap<>();

    /**
     * Creates the execution services.
     *
     * @param topSrvc Topology service.
     * @param msgSrvc Message service.
     * @param sqlSchemaManager Schema manager.
     * @param ddlCommandHandler Handler of the DDL commands.
     * @param taskExecutor Task executor.
     * @param handler Row handler.
     * @param mailboxRegistry Mailbox registry.
     * @param exchangeSrvc Exchange service.
     * @param <RowT> Type of the sql row.
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
            ExecutionDependencyResolver dependencyResolver
    ) {
        HashFunctionFactoryImpl<RowT> rowHashFunctionFactory = new HashFunctionFactoryImpl<>(handler);

        return new ExecutionServiceImpl<>(
                msgSrvc,
                topSrvc,
                mappingService,
                sqlSchemaManager,
                ddlCommandHandler,
                taskExecutor,
                handler,
                dependencyResolver,
                (ctx, deps) -> new LogicalRelImplementor<>(
                        ctx,
                        rowHashFunctionFactory,
                        mailboxRegistry,
                        exchangeSrvc,
                        deps)
        );
    }

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
     */
    public ExecutionServiceImpl(
            MessageService messageService,
            TopologyService topSrvc,
            MappingService mappingService,
            SqlSchemaManager sqlSchemaManager,
            DdlCommandHandler ddlCmdHnd,
            QueryTaskExecutor taskExecutor,
            RowHandler<RowT> handler,
            ExecutionDependencyResolver dependencyResolver,
            ImplementorFactory<RowT> implementorFactory
    ) {
        this.localNode = topSrvc.localMember();
        this.handler = handler;
        this.messageService = messageService;
        this.mappingService = mappingService;
        this.topSrvc = topSrvc;
        this.sqlSchemaManager = sqlSchemaManager;
        this.taskExecutor = taskExecutor;
        this.ddlCmdHnd = ddlCmdHnd;
        this.dependencyResolver = dependencyResolver;
        this.implementorFactory = implementorFactory;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        messageService.register((n, m) -> onMessage(n, (QueryStartRequest) m), SqlQueryMessageGroup.QUERY_START_REQUEST);
        messageService.register((n, m) -> onMessage(n, (QueryStartResponse) m), SqlQueryMessageGroup.QUERY_START_RESPONSE);
        messageService.register((n, m) -> onMessage(n, (QueryCloseMessage) m), SqlQueryMessageGroup.QUERY_CLOSE_MESSAGE);
        messageService.register((n, m) -> onMessage(n, (ErrorMessage) m), SqlQueryMessageGroup.ERROR_MESSAGE);
    }

    private AsyncCursor<List<Object>> executeQuery(
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

    private BaseQueryContext createQueryContext(UUID queryId, int schemaVersion, Object[] params) {
        DynamicParameterValue[] parameters = DynamicParameterValue.fromValues(params);

        return BaseQueryContext.builder()
                .queryId(queryId)
                .parameters(parameters)
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(sqlSchemaManager.schema(schemaVersion))
                                .build()
                )
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
    public AsyncCursor<List<Object>> executePlan(
            InternalTransaction tx, QueryPlan plan, BaseQueryContext ctx
    ) {
        SqlQueryType queryType = plan.type();
        assert queryType != null : "Root plan can not be a fragment";

        switch (queryType) {
            case DML:
                // TODO a barrier between previous operation and this one
            case QUERY:
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
            return CompletableFuture.completedFuture(null);
        }

        return mgr.close(true);
    }

    private AsyncCursor<List<Object>> executeDdl(DdlPlan plan, @Nullable QueryPrefetchCallback callback) {
        CompletableFuture<Iterator<List<Object>>> ret = ddlCmdHnd.handle(plan.command())
                .thenApply(applied -> List.of(List.<Object>of(applied)).iterator())
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
            return new IgniteInternalException(INTERNAL_ERR, "Failed to execute DDL statement [stmt=" /*+ qry.sql()*/
                    + ", err=" + e.getMessage() + ']', e);
        }

        return (e instanceof RuntimeException) ? (RuntimeException) e : new IgniteInternalException(INTERNAL_ERR, e);
    }

    private AsyncCursor<List<Object>> executeExplain(ExplainPlan plan, @Nullable QueryPrefetchCallback callback) {
        IgniteRel clonedRoot = Cloner.clone(plan.plan().root(), Commons.cluster());

        String planString = RelOptUtil.toString(clonedRoot, SqlExplainLevel.ALL_ATTRIBUTES);

        List<List<Object>> res = List.of(List.of(planString));

        if (callback != null) {
            taskExecutor.execute(() -> callback.onPrefetchComplete(null));
        }

        return new AsyncWrapper<>(res.iterator());
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
            // Using get() without a timeout to exclude situations when it's not clear whether the node has actually stopped or not.
            f.get();
        } catch (CancellationException e) {
            LOG.warn("The stop future was cancelled, going to proceed the stop procedure", e);
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
            BaseQueryContext ctx = createQueryContext(key, msg.schemaVersion(), msg.parameters());

            return new DistributedQueryManager(coordinatorNodeName, ctx);
        });
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

        private final @Nullable CompletableFuture<AsyncRootNode<RowT, List<Object>>> root;

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
                var root = new CompletableFuture<AsyncRootNode<RowT, List<Object>>>();

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
            DynamicParameterValue[] parameters = ctx.parameters();
            Object[] parameterValues = new Object[parameters.length];

            for (int i = 0; i < parameters.length; i++) {
                parameterValues[i] = parameters[i].value();
            }

            QueryStartRequest request = FACTORY.queryStartRequest()
                    .queryId(ctx.queryId())
                    .fragmentId(desc.fragmentId())
                    .root(serialisedFragment)
                    .fragmentDescription(desc)
                    .parameters(parameterValues)
                    .txAttributes(txAttributes)
                    .schemaVersion(ctx.schemaVersion())
                    .build();

            CompletableFuture<Void> remoteFragmentInitializationCompletionFuture = new CompletableFuture<>();

            remoteFragmentInitCompletion.put(
                    new RemoteFragmentKey(targetNodeName, desc.fragmentId()),
                    remoteFragmentInitializationCompletionFuture
            );

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
                Function<RowT, RowT> internalTypeConverter = TypeUtils.resultTypeConverter(ectx, treeRoot.getRowType());

                AsyncRootNode<RowT, List<Object>> rootNode = new AsyncRootNode<>(node, inRow -> {
                    inRow = internalTypeConverter.apply(inRow);

                    int rowSize = ectx.rowHandler().columnCount(inRow);

                    List<Object> res = new ArrayList<>(rowSize);

                    for (int i = 0; i < rowSize; i++) {
                        res.add(ectx.rowHandler().get(i, inRow));
                    }

                    return res;
                });
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
            DynamicParameterValue[] parameters = ctx.parameters();
            Object[] parameterValues = new Object[parameters.length];

            for (int i = 0; i < parameters.length; i++) {
                parameterValues[i] = parameters[i].value();
            }

            return new ExecutionContext<>(
                    taskExecutor,
                    ctx.queryId(),
                    localNode,
                    initiatorNodeName,
                    desc,
                    handler,
                    Commons.parametersMap(parameterValues),
                    txAttributes
            );
        }

        private void submitFragment(
                String initiatorNode,
                int schemaVersion,
                String fragmentString,
                FragmentDescription desc,
                TxAttributes txAttributes
        ) {
            // Because fragment execution runs on specific thread selected by taskExecutor,
            // we should complete dependency resolution on the same thread
            // that is going to be used for fragment execution.
            ExecutionContext<RowT> context = createContext(initiatorNode, desc, txAttributes);
            Executor exec = (r) -> context.execute(r::run, err -> handleError(err, initiatorNode, desc.fragmentId()));

            try {
                IgniteRel treeRoot = relationalTreeFromJsonString(schemaVersion, fragmentString, ctx);

                dependencyResolver.resolveDependencies(List.of(treeRoot), schemaVersion)
                        .thenComposeAsync(deps -> executeFragment(treeRoot, deps, context), exec)
                        .exceptionally(ex -> {
                            handleError(ex, initiatorNode, desc.fragmentId());

                            return null;
                        });
            } catch (Exception ex) {
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

        private AsyncCursor<List<Object>> execute(InternalTransaction tx, MultiStepPlan multiStepPlan) {
            assert root != null;

            mappingService.map(multiStepPlan).whenCompleteAsync((mappedFragments, mappingErr) -> {
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
                                mappedFragment.sourcesByExchangeId()
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
                public CompletableFuture<BatchedResult<List<Object>>> requestNextAsync(int rows) {
                    return root.thenCompose(cur -> {
                        var fut = cur.requestNextAsync(rows);

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
                    return root.thenCompose(none -> DistributedQueryManager.this.close(false));
                }
            };
        }

        private void enlistPartitions(MappedFragment mappedFragment, InternalTransaction tx) {
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
                    int tableId = rel.getTable().unwrap(IgniteTable.class).id();

                    List<NodeWithTerm> assignments = mappedFragment.groupsBySourceId()
                            .get(UpdatableTableImpl.MODIFY_NODE_SOURCE_ID).assignments();

                    assert assignments != null : "Table assignments must be available";

                    enlist(tableId, assignments);

                    return super.visit(rel);
                }

                private void enlist(int tableId, List<NodeWithTerm> assignments) {
                    if (assignments.isEmpty()) {
                        return;
                    }

                    int partsCnt = assignments.size();

                    tx.assignCommitPartition(new TablePartitionId(tableId, ThreadLocalRandom.current().nextInt(partsCnt)));

                    for (int p = 0; p < partsCnt; p++) {
                        TablePartitionId tablePartId = new TablePartitionId(tableId, p);

                        NodeWithTerm enlistmentToken = assignments.get(p);

                        tx.enlist(tablePartId,
                                new IgniteBiTuple<>(topSrvc.getByConsistentId(enlistmentToken.name()), enlistmentToken.term()));
                    }
                }

                private void enlist(SourceAwareIgniteRel rel) {
                    int tableId = rel.getTable().unwrap(IgniteTable.class).id();

                    List<NodeWithTerm> assignments = mappedFragment.groupsBySourceId().get(rel.sourceId()).assignments();

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
                AsyncRootNode<RowT, List<Object>> node = root.getNow(null);

                if (cancel) {
                    node.onError(new QueryCancelledException());
                }

                return node.closeAsync();
            }

            return Commons.completedFuture();
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
