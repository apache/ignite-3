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

import static org.apache.ignite.internal.sql.engine.externalize.RelJsonReader.fromJson;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.NodeLeftException;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
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
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.metadata.FragmentDescription;
import org.apache.ignite.internal.sql.engine.metadata.FragmentMapping;
import org.apache.ignite.internal.sql.engine.metadata.MappingService;
import org.apache.ignite.internal.sql.engine.metadata.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.metadata.NodeWithTerm;
import org.apache.ignite.internal.sql.engine.metadata.RemoteFragmentExecutionException;
import org.apache.ignite.internal.sql.engine.prepare.Cloner;
import org.apache.ignite.internal.sql.engine.prepare.DdlPlan;
import org.apache.ignite.internal.sql.engine.prepare.ExplainPlan;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
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
import org.apache.ignite.internal.sql.engine.util.HashFunctionFactoryImpl;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.internal.util.TransformingIterator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;

/**
 * ExecutionServiceImpl. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class ExecutionServiceImpl<RowT> implements ExecutionService, TopologyEventHandler {
    private static final int CACHE_SIZE = 1024;

    private final ConcurrentMap<String, IgniteRel> physNodesCache = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .<String, IgniteRel>build()
            .asMap();

    private static final IgniteLogger LOG = Loggers.forClass(ExecutionServiceImpl.class);

    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    private final MessageService messageService;

    private final TopologyService topSrvc;

    private final ClusterNode localNode;

    private final SqlSchemaManager sqlSchemaManager;

    private final QueryTaskExecutor taskExecutor;

    private final MappingService mappingSrvc;

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
            ExecutionDependencyResolver dependencyResolver
    ) {
        return new ExecutionServiceImpl<>(
                msgSrvc,
                topSrvc,
                new MappingServiceImpl(topSrvc),
                sqlSchemaManager,
                ddlCommandHandler,
                taskExecutor,
                handler,
                dependencyResolver,
                (ctx, deps) -> new LogicalRelImplementor<>(
                        ctx,
                        new HashFunctionFactoryImpl<>(sqlSchemaManager, handler),
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
     * @param mappingSrvc Nodes mapping calculation service.
     * @param sqlSchemaManager Schema manager.
     * @param ddlCmdHnd Handler of the DDL commands.
     * @param taskExecutor Task executor.
     * @param handler Row handler.
     * @param implementorFactory Relational node implementor factory.
     */
    public ExecutionServiceImpl(
            MessageService messageService,
            TopologyService topSrvc,
            MappingService mappingSrvc,
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
        this.mappingSrvc = mappingSrvc;
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

    @WithSpan
    private AsyncCursor<List<Object>> executeQuery(
            InternalTransaction tx,
            BaseQueryContext ctx,
            MultiStepPlan plan
    ) {
        DistributedQueryManager queryManager = new DistributedQueryManager(true, ctx);

        DistributedQueryManager old = queryManagerMap.put(ctx.queryId(), queryManager);

        assert old == null;

        ctx.cancel().add(() -> queryManager.close(true));

        return queryManager.execute(tx, plan);
    }

    private BaseQueryContext createQueryContext(UUID queryId, long schemaVersion, @Nullable String schema, Object[] params) {
        return BaseQueryContext.builder()
                .queryId(queryId)
                .parameters(params)
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(sqlSchemaManager.schema(schema, (int) schemaVersion))
                                .build()
                )
                .logger(LOG)
                .build();
    }

    private IgniteRel relationalTreeFromJsonString(String jsonFragment, BaseQueryContext ctx) {
        IgniteRel plan = physNodesCache.computeIfAbsent(jsonFragment, ser -> fromJson(ctx, ser));

        return new Cloner(Commons.cluster()).visit(plan);
    }

    /** {@inheritDoc} */
    @WithSpan
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
                return executeExplain((ExplainPlan) plan);
            case DDL:
                return executeDdl((DdlPlan) plan);

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

    @WithSpan
    private AsyncCursor<List<Object>> executeDdl(DdlPlan plan) {
        CompletableFuture<Iterator<List<Object>>> ret = ddlCmdHnd.handle(plan.command())
                .thenApply(applied -> List.of(List.<Object>of(applied)).iterator())
                .exceptionally(th -> {
                    throw convertDdlException(th);
                });

        return new AsyncWrapper<>(ret, Runnable::run);
    }

    private static RuntimeException convertDdlException(Throwable e) {
        if (e instanceof CompletionException) {
            e = e.getCause();
        }

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

    @WithSpan
    private AsyncCursor<List<Object>> executeExplain(ExplainPlan plan) {
        List<List<Object>> res = List.of(List.of(plan.plan()));

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
        f.join();
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
        DistributedQueryManager queryManager = getOrCreateQueryManager(msg);

        queryManager.submitFragment(nodeName, msg.root(), msg.fragmentDescription(), msg.txAttributes());
    }

    private void handleError(Throwable ex, String nodeName, QueryStartRequest msg) {
        DistributedQueryManager queryManager = getOrCreateQueryManager(msg);

        queryManager.handleError(ex, nodeName, msg.fragmentDescription().fragmentId());
    }

    private DistributedQueryManager getOrCreateQueryManager(QueryStartRequest msg) {
        return queryManagerMap.computeIfAbsent(msg.queryId(), key -> {
            BaseQueryContext ctx = createQueryContext(key, msg.schemaVersion(), msg.schema(), msg.parameters());

            return new DistributedQueryManager(ctx);
        });
    }

    /**
     * A convenient class that manages the initialization and termination of distributed queries.
     */
    private class DistributedQueryManager {
        private final boolean coordinator;

        private final BaseQueryContext ctx;

        private final CompletableFuture<Void> cancelFut = new CompletableFuture<>();

        private final AtomicBoolean cancelled = new AtomicBoolean();

        private final Map<RemoteFragmentKey, CompletableFuture<Void>> remoteFragmentInitCompletion = new ConcurrentHashMap<>();

        private final Queue<AbstractNode<RowT>> localFragments = new LinkedBlockingQueue<>();

        private final CompletableFuture<AsyncRootNode<RowT, List<Object>>> root;

        private volatile Long rootFragmentId = null;

        private DistributedQueryManager(boolean coordinator, BaseQueryContext ctx) {
            this.ctx = ctx;
            this.coordinator = coordinator;

            var root = new CompletableFuture<AsyncRootNode<RowT, List<Object>>>();

            root.exceptionally(t -> {
                this.close(true);

                return null;
            });

            this.root = root;
        }

        private DistributedQueryManager(BaseQueryContext ctx) {
            this(false, ctx);
        }

        private List<AbstractNode<?>> localFragments() {
            return List.copyOf(localFragments);
        }

        @WithSpan
        private CompletableFuture<Void> sendFragment(
                String targetNodeName, Fragment fragment, FragmentDescription desc, TxAttributes txAttributes
        ) {
            QueryStartRequest request = FACTORY.queryStartRequest()
                    .queryId(ctx.queryId())
                    .fragmentId(fragment.fragmentId())
                    .schema(ctx.schemaName())
                    .root(fragment.serialized())
                    .fragmentDescription(desc)
                    .parameters(ctx.parameters())
                    .txAttributes(txAttributes)
                    .schemaVersion(ctx.schemaVersion())
                    .build();

            CompletableFuture<Void> remoteFragmentInitializationCompletionFuture = new CompletableFuture<>();

            remoteFragmentInitCompletion.put(
                    new RemoteFragmentKey(targetNodeName, fragment.fragmentId()),
                    remoteFragmentInitializationCompletionFuture
            );

            return messageService.send(targetNodeName, request);
        }

        @WithSpan
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

        @WithSpan
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

                rootNode.prefetch();

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
                    ctx,
                    taskExecutor,
                    ctx.queryId(),
                    localNode,
                    initiatorNodeName,
                    desc,
                    handler,
                    Commons.parametersMap(ctx.parameters()),
                    txAttributes
            );
        }

        @WithSpan
        private void submitFragment(
                String initiatorNode,
                String fragmentString,
                FragmentDescription desc,
                TxAttributes txAttributes
        ) {
            CompletableFuture<?> start = new CompletableFuture<>();
            // Because fragment execution runs on specific thread selected by taskExecutor,
            // we should complete dependency resolution on the same thread
            // that is going to be used for fragment execution.
            ExecutionContext<RowT> context = createContext(initiatorNode, desc, txAttributes);
            Executor exec = (r) -> context.execute(r::run, err -> handleError(err, initiatorNode, desc.fragmentId()));

            start.thenCompose(none -> {
                IgniteRel treeRoot = relationalTreeFromJsonString(fragmentString, ctx);
                long schemaVersion = ctx.schemaVersion();

                return dependencyResolver.resolveDependencies(List.of(treeRoot), schemaVersion).thenComposeAsync(deps -> {
                    return executeFragment(treeRoot, deps, context);
                }, exec);
            }).exceptionally(ex -> {
                handleError(ex, initiatorNode, desc.fragmentId());

                return null;
            });

            start.complete(null);
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

        @WithSpan
        private AsyncCursor<List<Object>> execute(InternalTransaction tx, MultiStepPlan notMappedPlan) {
            CompletableFuture<MultiStepPlan> f = mapFragments(notMappedPlan);

            f.whenCompleteAsync((plan, mappingErr) -> {
                if (mappingErr != null) {
                    if (!root.completeExceptionally(mappingErr)) {
                        root.thenAccept(root -> root.onError(mappingErr));
                    }
                    return;
                }

                try {
                    List<Fragment> fragments = plan.fragments();

                    // we rely on the fact that the very first fragment is a root. Otherwise we need to handle
                    // the case when a non-root fragment will fail before the root is processed.
                    assert !nullOrEmpty(fragments) && fragments.get(0).rootFragment() : fragments;

                    // first let's enlist all tables to the transaction.
                    if (!tx.isReadOnly()) {
                        for (Fragment fragment : fragments) {
                            enlistPartitions(fragment, tx);
                        }
                    }

                    // now transaction is initialized for sure including assignment
                    // of the commit partition (if there is at least one table in the fragments)
                    TxAttributes attributes = TxAttributes.fromTx(tx);

                    List<CompletableFuture<?>> resultsOfFragmentSending = new ArrayList<>();

                    // start remote execution
                    for (Fragment fragment : fragments) {
                        if (fragment.rootFragment()) {
                            assert rootFragmentId == null;

                            rootFragmentId = fragment.fragmentId();
                        }

                        FragmentDescription fragmentDesc = new FragmentDescription(
                                fragment.fragmentId(),
                                !fragment.correlated(),
                                plan.mapping(fragment),
                                plan.target(fragment),
                                plan.remotes(fragment)
                        );

                        for (String nodeName : fragmentDesc.nodeNames()) {
                            CompletableFuture<Void> resultOfSending = sendFragment(nodeName, fragment, fragmentDesc, attributes);

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

                                        throw ExceptionUtils.withCauseAndCode(
                                                IgniteInternalException::new,
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
                @WithSpan
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
                @WithSpan
                @Override
                public CompletableFuture<Void> closeAsync() {
                    return root.thenCompose(none -> DistributedQueryManager.this.close(false));
                }
            };
        }

        private void enlistPartitions(Fragment fragment, InternalTransaction tx) {
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
                    List<NodeWithTerm> assignments = fragment.mapping().updatingTableAssignments();

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
                        NodeWithTerm leaderWithTerm = assignments.get(p);

                        tx.enlist(new TablePartitionId(tableId, p),
                                new IgniteBiTuple<>(topSrvc.getByConsistentId(leaderWithTerm.name()), leaderWithTerm.term()));
                    }
                }

                private void enlist(SourceAwareIgniteRel rel) {
                    int tableId = rel.getTable().unwrap(IgniteTable.class).id();
                    List<NodeWithTerm> assignments = fragment.mapping().findGroup(rel.sourceId()).assignments().stream()
                            .map(l -> l.get(0))
                            .collect(Collectors.toList());

                    enlist(tableId, assignments);
                }
            }.visit(fragment.root());
        }

        private CompletableFuture<MultiStepPlan> mapFragments(MultiStepPlan plan) {
            Iterable<IgniteRel> fragments = TransformingIterator.newIterable(plan.fragments(), (f) -> f.root());

            CompletableFuture<ResolvedDependencies> fut = dependencyResolver.resolveDependencies(fragments,
                    ctx.schemaVersion());

            return fut.thenCompose(deps -> {
                return fetchColocationGroups(deps).thenApply(colocationGroups -> {
                    MappingQueryContext mappingCtx = new MappingQueryContext(localNode.name(), mappingSrvc);
                    List<Fragment> mappedFragments = FragmentMapping.mapFragments(mappingCtx, plan.fragments(), colocationGroups);

                    return plan.replaceFragments(mappedFragments);
                });
            });
        }

        @WithSpan
        private CompletableFuture<Void> close(boolean cancel) {
            if (!cancelled.compareAndSet(false, true)) {
                return cancelFut;
            }

            CompletableFuture<Void> start = closeExecNode(cancel);

            start
                    .thenCompose(tmp -> {
                        CompletableFuture<Void> cancelResult = coordinator
                                ? awaitFragmentInitialisationAndClose()
                                : closeLocalFragments();

                        var finalStepFut = cancelResult.whenComplete((r, e) -> {
                            if (e != null) {
                                Throwable ex = unwrapCause(e);

                                LOG.warn("Fragment closing processed with errors: [queryId={}]", ex, ctx.queryId());
                            }

                            queryManagerMap.remove(ctx.queryId());

                            try {
                                ctx.cancel().cancel();
                            } catch (Exception th) {
                                LOG.debug("Exception raised while cancel", th);
                            }

                            cancelFut.complete(null);
                        });

                        return cancelResult.thenCombine(finalStepFut, (none1, none2) -> null);
                    })
                    .thenRun(() -> localFragments.forEach(f -> f.context().cancel()));

            start.completeAsync(() -> null, taskExecutor);

            return cancelFut;
        }

        @WithSpan
        private CompletableFuture<Void> closeLocalFragments() {
            ExecutionCancelledException ex = new ExecutionCancelledException();

            List<CompletableFuture<?>> localFragmentCompletions = new ArrayList<>();
            for (AbstractNode<?> node : localFragments) {
                assert !node.context().isCancelled() : "node context is cancelled, but node still processed";

                localFragmentCompletions.add(
                        node.context().submit(() -> node.onError(ex), node::onError)
                );
            }

            return CompletableFuture.allOf(
                    localFragmentCompletions.toArray(new CompletableFuture[0])
            );
        }

        @WithSpan
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
         * @param cancel Forces execution to terminate with {@link ExecutionCancelledException}.
         * @return Completable future that should run asynchronously.
         */
        @WithSpan
        private CompletableFuture<Void> closeExecNode(boolean cancel) {
            CompletableFuture<Void> start = new CompletableFuture<>();

            if (!root.completeExceptionally(new ExecutionCancelledException()) && !root.isCompletedExceptionally()) {
                AsyncRootNode<RowT, List<Object>> node = root.getNow(null);

                if (!cancel) {
                    CompletableFuture<Void> closeFut = node.closeAsync();

                    return start.thenCompose(v -> closeFut);
                }

                node.onError(new ExecutionCancelledException());
            }

            return start;
        }

        @WithSpan
        private CompletableFuture<Map<Integer, ColocationGroup>> fetchColocationGroups(ResolvedDependencies deps) {
            List<CompletableFuture<Pair<Integer, ColocationGroup>>> list = new ArrayList<>();

            for (Integer tableId : deps.tableIds()) {
                CompletableFuture<ColocationGroup> f = deps.fetchColocationGroup(tableId);
                list.add(f.thenApply(c -> new Pair<>(tableId, c)));
            }

            CompletableFuture<Void> all = CompletableFuture.allOf(list.toArray(new CompletableFuture[0]));

            return all.thenApply(
                    v -> list.stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));
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
}
