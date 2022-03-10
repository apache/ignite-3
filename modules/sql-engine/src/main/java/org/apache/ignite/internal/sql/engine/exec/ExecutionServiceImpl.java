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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.sql.engine.externalize.RelJsonReader.fromJson;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.RemoteFragmentKey;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.rel.AbstractNode;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.sql.engine.exec.rel.RootNode;
import org.apache.ignite.internal.sql.engine.message.ErrorMessage;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.QueryCloseMessage;
import org.apache.ignite.internal.sql.engine.message.QueryStartRequest;
import org.apache.ignite.internal.sql.engine.message.QueryStartResponse;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.sql.engine.metadata.AffinityService;
import org.apache.ignite.internal.sql.engine.metadata.FragmentDescription;
import org.apache.ignite.internal.sql.engine.metadata.MappingService;
import org.apache.ignite.internal.sql.engine.metadata.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.metadata.RemoteException;
import org.apache.ignite.internal.sql.engine.prepare.DdlPlan;
import org.apache.ignite.internal.sql.engine.prepare.ExplainPlan;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.FragmentPlan;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.NodeLeaveHandler;
import org.apache.ignite.internal.sql.engine.util.TransformingIterator;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;

/**
 * ExecutionServiceImpl. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class ExecutionServiceImpl<RowT> implements ExecutionService {
    private static final IgniteLogger LOG = IgniteLogger.forClass(ExecutionServiceImpl.class);

    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    private final TopologyService topSrvc;

    private final MessageService msgSrvc;

    private final String locNodeId;

    private final SqlSchemaManager sqlSchemaManager;

    private final QueryTaskExecutor taskExecutor;

    private final AffinityService affSrvc;

    private final MailboxRegistry mailboxRegistry;

    private final MappingService mappingSrvc;

    private final ExchangeService exchangeSrvc;

    private final ClosableIteratorsHolder iteratorsHolder;

    private final RowHandler<RowT> handler;

    private final DdlCommandHandler ddlCmdHnd;

    private final Map<UUID, DistributedQueryManager> queryManagerMap = new ConcurrentHashMap<>();

    /**
     * Constructor. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ExecutionServiceImpl(
            TopologyService topSrvc,
            MessageService msgSrvc,
            SqlSchemaManager sqlSchemaManager,
            TableManager tblManager,
            QueryTaskExecutor taskExecutor,
            RowHandler<RowT> handler,
            MailboxRegistry mailboxRegistry,
            ExchangeService exchangeSrvc
    ) {
        this.topSrvc = topSrvc;
        this.handler = handler;
        this.msgSrvc = msgSrvc;
        this.sqlSchemaManager = sqlSchemaManager;
        this.taskExecutor = taskExecutor;
        this.mailboxRegistry = mailboxRegistry;
        this.exchangeSrvc = exchangeSrvc;

        ddlCmdHnd = new DdlCommandHandler(tblManager);

        locNodeId = topSrvc.localMember().id();
        iteratorsHolder = new ClosableIteratorsHolder(topSrvc.localMember().name(), LOG);
        mappingSrvc = new MappingServiceImpl(topSrvc);
        // TODO: fix this
        affSrvc = cacheId -> Objects::hashCode;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        iteratorsHolder.start();
        topSrvc.addEventHandler(new NodeLeaveHandler(this::onNodeLeft));

        msgSrvc.register((n, m) -> onMessage(n, (QueryStartRequest) m), SqlQueryMessageGroup.QUERY_START_REQUEST);
        msgSrvc.register((n, m) -> onMessage(n, (QueryStartResponse) m), SqlQueryMessageGroup.QUERY_START_RESPONSE);
        msgSrvc.register((n, m) -> onMessage(n, (QueryCloseMessage) m), SqlQueryMessageGroup.QUERY_CLOSE_MESSAGE);
        msgSrvc.register((n, m) -> onMessage(n, (ErrorMessage) m), SqlQueryMessageGroup.ERROR_MESSAGE);
    }

    private AsyncCursor<List<?>> executeQuery(
            BaseQueryContext ctx,
            MultiStepPlan plan
    ) {
        DistributedQueryManager queryManager;

        DistributedQueryManager old = queryManagerMap.put(ctx.queryId(), queryManager = new DistributedQueryManager(ctx));

        assert old == null;

        return new AsyncWrapper<>(queryManager.execute(plan));
    }

    private BaseQueryContext createQueryContext(UUID queryId, @Nullable String schema, Object[] params) {
        return BaseQueryContext.builder()
                .queryId(queryId)
                .parameters(params)
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(sqlSchemaManager.schema(schema))
                                .build()
                )
                .logger(LOG)
                .build();
    }

    private QueryPlan prepareFragment(String jsonFragment) {
        return new FragmentPlan(fromJson(sqlSchemaManager, jsonFragment));
    }

    /** {@inheritDoc} */
    @Override
    public AsyncCursor<List<?>> executePlan(
            QueryPlan plan, BaseQueryContext ctx
    ) {
        switch (plan.type()) {
            case DML:
                // TODO a barrier between previous operation and this one
            case QUERY:
                return executeQuery(
                        ctx,
                        (MultiStepPlan) plan
                );
            case EXPLAIN:
                return executeExplain((ExplainPlan) plan);
            case DDL:
                return executeDdl((DdlPlan) plan);

            default:
                throw new AssertionError("Unexpected plan type: " + plan);
        }
    }

    private AsyncCursor<List<?>> executeDdl(DdlPlan plan) {
        try {
            ddlCmdHnd.handle(plan.command());
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException("Failed to execute DDL statement [stmt=" /*+ qry.sql()*/
                    + ", err=" + e.getMessage() + ']', e);
        }

        return new AsyncWrapper<>(Collections.emptyIterator());
    }

    private AsyncCursor<List<?>> executeExplain(ExplainPlan plan) {
        List<List<?>> res = List.of(List.of(plan.plan()));

        return new AsyncWrapper<>(res.iterator());
    }

    private void onMessage(String nodeId, QueryStartRequest msg) {
        assert nodeId != null && msg != null;

        DistributedQueryManager queryManager = queryManagerMap.computeIfAbsent(msg.queryId(), key -> {
            BaseQueryContext ctx = createQueryContext(key, msg.schema(), msg.parameters());

            return new DistributedQueryManager(ctx);
        });

        queryManager.submitFragment(nodeId, msg.root(), msg.fragmentDescription());
    }

    private void onMessage(String nodeId, QueryStartResponse msg) {
        assert nodeId != null && msg != null;

        DistributedQueryManager dqm = queryManagerMap.get(msg.queryId());

        if (dqm != null) {
            dqm.acknowledgeFragment(nodeId, msg.fragmentId(), msg.error());
        }
    }

    private void onMessage(String nodeId, ErrorMessage msg) {
        assert nodeId != null && msg != null;

        DistributedQueryManager dqm = queryManagerMap.get(msg.queryId());

        if (dqm != null) {
            RemoteException e = new RemoteException(nodeId, msg.queryId(), msg.fragmentId(), msg.error());

            dqm.onError(e);
        }
    }

    private void onMessage(String nodeId, QueryCloseMessage msg) {
        assert nodeId != null && msg != null;

        DistributedQueryManager dqm = queryManagerMap.get(msg.queryId());

        if (dqm != null) {
            dqm.close();
        }
    }

    private void onNodeLeft(ClusterNode node) {
        queryManagerMap.values().forEach(qm -> qm.onNodeLeft(node.id()));
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        IgniteUtils.closeAll(iteratorsHolder::stop);
    }

    static class AsyncWrapper<T> implements AsyncCursor<T> {
        private final AtomicReference<CompletableFuture<Iterator<T>>> cursorRef;

        public AsyncWrapper(Iterator<T> source) {
            this(CompletableFuture.completedFuture(source));
        }

        public AsyncWrapper(CompletableFuture<Iterator<T>> initFut) {
            this.cursorRef = new AtomicReference<>(initFut);
        }

        @Override
        public CompletionStage<List<T>> requestNext(int rows) {
            CompletableFuture<List<T>> batchFut = new CompletableFuture<>();

            cursorRef.updateAndGet(fut -> fut.thenApplyAsync(cursor -> {
                int remains = rows;
                List<T> batch = new ArrayList<>(rows);

                while (remains-- > 0 && cursor.hasNext()) {
                    batch.add(cursor.next());
                }

                batchFut.complete(batch);

                return cursor;
            }).exceptionally(t -> {
                batchFut.completeExceptionally(t);

                return null;
            }));

            return batchFut;
        }

        @Override
        public CompletableFuture<Void> close() {
            return cursorRef.get().thenAccept(iterator -> {
                if (iterator instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) iterator).close();
                    } catch (Exception e) {
                        throw new IgniteInternalException(e);
                    }
                }
            });
        }
    }

    /**
     * A convenient class that manages the initialization and termination of distributed queries.
     */
    class DistributedQueryManager {
        private final BaseQueryContext ctx;

        private final AtomicReference<CompletableFuture<Void>> cancelFut = new AtomicReference<>();

        private final Map<RemoteFragmentKey, CompletableFuture<Void>> remoteFragmentInitCompletion = new ConcurrentHashMap<>();

        private final CompletableFuture<RootNode<RowT>> root = new CompletableFuture<>();

        private final Queue<AbstractNode<RowT>> localFragments = new LinkedBlockingQueue<>();

        private volatile Long rootFragmentId = null;

        public DistributedQueryManager(BaseQueryContext ctx) {
            this.ctx = ctx;
        }

        private void sendFragment(String targetNodeId, Fragment fragment, FragmentDescription desc) throws IgniteInternalCheckedException {
            QueryStartRequest req = FACTORY.queryStartRequest()
                    .queryId(ctx.queryId())
                    .fragmentId(fragment.fragmentId())
                    .schema(ctx.schemaName())
                    .root(fragment.serialized())
                    .fragmentDescription(desc)
                    .parameters(ctx.parameters())
                    .build();

            msgSrvc.send(targetNodeId, req);

            remoteFragmentInitCompletion.put(new RemoteFragmentKey(targetNodeId, fragment.fragmentId()), new CompletableFuture<>());
        }

        private void acknowledgeFragment(String nodeId, long fragmentId, @Nullable Throwable ex) {
            if (ex != null) {
                Long rootFragmentId0 = rootFragmentId;

                if (rootFragmentId0 != null && fragmentId == rootFragmentId0) {
                    root.completeExceptionally(ex);
                } else {
                    root.thenAccept(root -> root.onError(ex));
                }
            }

            remoteFragmentInitCompletion.get(new RemoteFragmentKey(nodeId, fragmentId)).complete(null);
        }

        private void onError(RemoteException ex) {
            root.thenAccept(root -> root.onError(ex));
        }

        void onNodeLeft(String nodeId) {
            remoteFragmentInitCompletion.entrySet().stream().filter(e -> nodeId.equals(e.getKey().nodeId()))
                    .forEach(e -> e.getValue().completeExceptionally(new IgniteInternalException("asddd")));
        }

        private void executeFragment(FragmentPlan plan, ExecutionContext<RowT> ectx) {
            String origNodeId = ectx.originatingNodeId();

            AbstractNode<RowT> node = new LogicalRelImplementor<>(
                    ectx,
                    affSrvc,
                    mailboxRegistry,
                    exchangeSrvc
            ).go(plan.root());

            if (!(node instanceof Outbox)) {
                RootNode<RowT> rootNode = new RootNode<>(ectx, plan.root().getRowType(), this::close);
                rootNode.register(node);

                root.complete(rootNode);
            }

            localFragments.add(node);

            try {
                msgSrvc.send(
                        origNodeId,
                        FACTORY.queryStartResponse()
                                .queryId(ectx.queryId())
                                .fragmentId(ectx.fragmentId())
                                .build()
                );
            } catch (IgniteInternalCheckedException e) {
                throw new IgniteInternalException("Failed to send reply. [nodeId=" + origNodeId + ']', e);
            }

            if (node instanceof Outbox) {
                ((Outbox<?>) node).init();
            }
        }

        private ExecutionContext<RowT> createContext(String initiatorNodeId, FragmentDescription desc) {
            return new ExecutionContext<>(
                    ctx,
                    taskExecutor,
                    ctx.queryId(),
                    locNodeId,
                    initiatorNodeId,
                    desc,
                    handler,
                    Commons.parametersMap(ctx.parameters())
            );
        }

        void submitFragment(String initiatorNode, String fragmentString, FragmentDescription desc) {
            try {
                QueryPlan qryPlan = prepareFragment(fragmentString);

                FragmentPlan plan = (FragmentPlan) qryPlan;

                executeFragment(plan, createContext(initiatorNode, desc));
            } catch (Throwable ex) {
                LOG.error("Failed to start query fragment", ex);

                try {
                    msgSrvc.send(
                            initiatorNode,
                            FACTORY.queryStartResponse()
                                    .queryId(ctx.queryId())
                                    .fragmentId(desc.fragmentId())
                                    .error(ex)
                                    .build()
                    );
                } catch (Exception e) {
                    LOG.error("Error occurred during send error message", e);

                    close();
                }
            }
        }

        CompletableFuture<Iterator<List<?>>> execute(MultiStepPlan plan) {
            return CompletableFuture.runAsync(() -> {
                plan.init(mappingSrvc, new MappingQueryContext(locNodeId));

                List<Fragment> fragments = plan.fragments();

                // start remote execution
                try {
                    for (Fragment fragment : fragments) {
                        if (fragment.rootFragment()) {
                            assert rootFragmentId == null;

                            rootFragmentId = fragment.fragmentId();
                        }

                        FragmentDescription fragmentDesc = new FragmentDescription(
                                fragment.fragmentId(),
                                plan.mapping(fragment),
                                plan.target(fragment),
                                plan.remotes(fragment)
                        );

                        for (String nodeId : fragmentDesc.nodeIds()) {
                            sendFragment(nodeId, fragment, fragmentDesc);
                        }
                    }
                } catch (Throwable e) {
                    root.thenAccept(root -> root.onError(e));
                }
            }, taskExecutor)
                    .thenCompose(tmp -> root)
                    .thenApply(root -> new TransformingIterator<>(iteratorsHolder.iterator(root), row -> {
                        int rowSize = root.context().rowHandler().columnCount(row);

                        List<Object> res = new ArrayList<>(rowSize);

                        for (int i = 0; i < rowSize; i++) {
                            res.add(root.context().rowHandler().get(i, row));
                        }

                        return res;
                    }));
        }

        CompletableFuture<Void> close() {
            return cancelFut.updateAndGet(old -> {
                if (old != null) {
                    return old;
                }

                CompletableFuture<Void> start = new CompletableFuture<>();

                CompletableFuture<Void> end = start
                        .thenCompose(tmp -> {
                            if (!root.completeExceptionally(new ExecutionCancelledException())) {
                                return root.thenAccept(RootNode::closeInternal);
                            }

                            return CompletableFuture.completedFuture(null);
                        })
                        .thenCompose(tmp -> {
                            Map<String, List<CompletableFuture<?>>> requestsPerNode = new HashMap<>();
                            for (Map.Entry<RemoteFragmentKey, CompletableFuture<Void>> entry : remoteFragmentInitCompletion.entrySet()) {
                                requestsPerNode.computeIfAbsent(entry.getKey().nodeId(), key -> new ArrayList<>()).add(entry.getValue());
                            }

                            List<CompletableFuture<?>> cancelFuts = new ArrayList<>();
                            for (Map.Entry<String, List<CompletableFuture<?>>> entry : requestsPerNode.entrySet()) {
                                String nodeId = entry.getKey();

                                if (!exchangeSrvc.alive(nodeId)) {
                                    continue;
                                }

                                cancelFuts.add(
                                        CompletableFuture.allOf(entry.getValue().toArray(new CompletableFuture[0]))
                                                .thenRun(() -> {
                                                    try {
                                                        exchangeSrvc.closeQuery(nodeId, ctx.queryId());
                                                    } catch (IgniteInternalCheckedException e) {
                                                        throw new IgniteInternalException(
                                                                "Failed to send cancel message. [nodeId=" + nodeId + ']', e);
                                                    }
                                                })
                                );
                            }

                            for (AbstractNode<?> node : localFragments) {
                                node.context().execute(() -> {
                                    node.close();
                                    node.context().cancel();
                                }, node::onError);
                            }

                            return CompletableFuture.allOf(cancelFuts.toArray(new CompletableFuture[0]))
                                    .thenRun(() -> queryManagerMap.remove(ctx.queryId()));
                        });

                start.completeAsync(() -> null, taskExecutor);

                return end;
            });
        }
    }
}
