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

import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.sql.engine.externalize.RelJsonReader.fromJson;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.util.CollectionUtils.first;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.sql.engine.Query;
import org.apache.ignite.internal.sql.engine.QueryRegistry;
import org.apache.ignite.internal.sql.engine.QueryState;
import org.apache.ignite.internal.sql.engine.RootQuery;
import org.apache.ignite.internal.sql.engine.RunningFragment;
import org.apache.ignite.internal.sql.engine.RunningQuery;
import org.apache.ignite.internal.sql.engine.SqlCursor;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.Node;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.sql.engine.message.ErrorMessage;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.QueryStartRequest;
import org.apache.ignite.internal.sql.engine.message.QueryStartResponse;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.sql.engine.metadata.AffinityService;
import org.apache.ignite.internal.sql.engine.metadata.FragmentDescription;
import org.apache.ignite.internal.sql.engine.metadata.FragmentMapping;
import org.apache.ignite.internal.sql.engine.metadata.MappingService;
import org.apache.ignite.internal.sql.engine.metadata.MappingServiceImpl;
import org.apache.ignite.internal.sql.engine.metadata.RemoteException;
import org.apache.ignite.internal.sql.engine.prepare.CacheKey;
import org.apache.ignite.internal.sql.engine.prepare.DdlPlan;
import org.apache.ignite.internal.sql.engine.prepare.ExplainPlan;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.FragmentPlan;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlanCache;
import org.apache.ignite.internal.sql.engine.prepare.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.engine.prepare.ResultSetMetadataInternal;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.NodeLeaveHandler;
import org.apache.ignite.internal.sql.engine.util.TransformingIterator;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.storage.DataStorageManager;
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
public class ExecutionServiceImpl<RowT> implements ExecutionService<RowT> {
    private static final IgniteLogger LOG = IgniteLogger.forClass(ExecutionServiceImpl.class);

    private static final SqlQueryMessagesFactory FACTORY = new SqlQueryMessagesFactory();

    private final TopologyService topSrvc;

    private final MessageService msgSrvc;

    private final String locNodeId;

    private final QueryPlanCache qryPlanCache;

    private final SqlSchemaManager sqlSchemaManager;

    private final QueryTaskExecutor taskExecutor;

    private final AffinityService affSrvc;

    private final MailboxRegistry mailboxRegistry;

    private final MappingService mappingSrvc;

    private final ExchangeService exchangeSrvc;

    private final ClosableIteratorsHolder iteratorsHolder;

    private final QueryRegistry queryRegistry;

    private final RowHandler<RowT> handler;

    private final DdlCommandHandler ddlCmdHnd;

    /**
     * Constructor. TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ExecutionServiceImpl(
            TopologyService topSrvc,
            MessageService msgSrvc,
            QueryPlanCache planCache,
            SqlSchemaManager sqlSchemaManager,
            TableManager tblManager,
            QueryTaskExecutor taskExecutor,
            RowHandler<RowT> handler,
            MailboxRegistry mailboxRegistry,
            ExchangeService exchangeSrvc,
            QueryRegistry queryRegistry,
            DataStorageManager dataStorageManager
    ) {
        this.topSrvc = topSrvc;
        this.handler = handler;
        this.msgSrvc = msgSrvc;
        this.sqlSchemaManager = sqlSchemaManager;
        this.taskExecutor = taskExecutor;
        this.mailboxRegistry = mailboxRegistry;
        this.exchangeSrvc = exchangeSrvc;
        this.queryRegistry = queryRegistry;

        ddlCmdHnd = new DdlCommandHandler(tblManager, dataStorageManager);

        locNodeId = topSrvc.localMember().id();
        qryPlanCache = planCache;
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
        msgSrvc.register((n, m) -> onMessage(n, (ErrorMessage) m), SqlQueryMessageGroup.ERROR_MESSAGE);
    }

    private SqlCursor<List<?>> mapAndExecutePlan(
            RootQuery<RowT> qry,
            MultiStepPlan plan
    ) {
        qry.mapping();
        plan.init(mappingSrvc, new MappingQueryContext(locNodeId, topologyVersion()));

        List<Fragment> fragments = plan.fragments();

        // Local execution
        Fragment fragment = first(fragments);

        if (IgniteUtils.assertionsEnabled()) {
            assert fragment != null;

            FragmentMapping mapping = plan.mapping(fragment);

            assert mapping != null;

            List<String> nodes = mapping.nodeIds();

            assert nodes != null && nodes.size() == 1 && first(nodes).equals(locNodeId);
        }

        FragmentDescription fragmentDesc = new FragmentDescription(
                fragment.fragmentId(),
                plan.mapping(fragment),
                plan.target(fragment),
                plan.remotes(fragment));

        ExecutionContext<RowT> ectx = new ExecutionContext<>(
                qry.context(),
                taskExecutor,
                qry.id(),
                locNodeId,
                locNodeId,
                topologyVersion(),
                fragmentDesc,
                handler,
                Commons.parametersMap(qry.parameters()));

        Node<RowT> node = new LogicalRelImplementor<>(ectx, affSrvc, mailboxRegistry,
                exchangeSrvc).go(fragment.root());

        qry.run(ectx, plan, node);

        // start remote execution
        for (int i = 1; i < fragments.size(); i++) {
            fragment = fragments.get(i);
            fragmentDesc = new FragmentDescription(
                    fragment.fragmentId(),
                    plan.mapping(fragment),
                    plan.target(fragment),
                    plan.remotes(fragment));

            Throwable ex = null;
            for (String nodeId : fragmentDesc.nodeIds()) {
                if (ex != null) {
                    qry.onResponse(nodeId, fragment.fragmentId(), ex);
                } else {
                    try {
                        QueryStartRequest req = FACTORY.queryStartRequest()
                                .queryId(qry.id())
                                .fragmentId(fragment.fragmentId())
                                .schema(qry.context().schemaName())
                                .root(fragment.serialized())
                                .topologyVersion(ectx.topologyVersion())
                                .fragmentDescription(fragmentDesc)
                                .parameters(qry.parameters())
                                .build();

                        msgSrvc.send(nodeId, req);
                    } catch (Throwable e) {
                        qry.onResponse(nodeId, fragment.fragmentId(), ex = e);
                    }
                }
            }
        }

        return Commons.createCursor(new TransformingIterator<>(iteratorsHolder.iterator(qry.iterator()), row -> {
            int rowSize = ectx.rowHandler().columnCount(row);

            List<Object> res = new ArrayList<>(rowSize);

            for (int i = 0; i < rowSize; i++) {
                res.add(ectx.rowHandler().get(i, row));
            }

            return res;
        }), plan);
    }

    protected long topologyVersion() {
        return 1L;
    }

    private BaseQueryContext createQueryContext(Context parent, @Nullable String schema) {
        return BaseQueryContext.builder()
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
    public SqlCursor<List<?>> executePlan(
            RootQuery<RowT> qry,
            QueryPlan plan
    ) {
        switch (plan.type()) {
            case DML:
                // TODO a barrier between previous operation and this one
            case QUERY:
                return mapAndExecutePlan(
                        qry,
                        (MultiStepPlan) plan
                );
            case EXPLAIN:
                return executeExplain((ExplainPlan) plan);
            case DDL:
                return executeDdl(qry, (DdlPlan) plan);

            default:
                throw new AssertionError("Unexpected plan type: " + plan);
        }
    }

    private SqlCursor<List<?>> executeDdl(RootQuery<RowT> qry, DdlPlan plan) {
        try {
            ddlCmdHnd.handle(plan.command());
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException("Failed to execute DDL statement [stmt=" + qry.sql()
                    + ", err=" + e.getMessage() + ']', e);
        } finally {
            queryRegistry.unregister(qry.id());
        }

        return Commons.createCursor(Collections.emptyIterator(), plan);
    }

    private SqlCursor<List<?>> executeExplain(ExplainPlan plan) {
        SqlCursor<List<?>> cur = Commons.createCursor(singletonList(singletonList(plan.plan())), plan);
        // TODO: fix this
        //        cur.fieldsMeta(plan.fieldsMeta().queryFieldsMetadata(pctx.typeFactory()));

        return cur;
    }

    private void executeFragment(Query<RowT> qry, FragmentPlan plan, ExecutionContext<RowT> ectx) {
        String origNodeId = ectx.originatingNodeId();

        Outbox<RowT> node = new LogicalRelImplementor<>(
                ectx,
                affSrvc,
                mailboxRegistry,
                exchangeSrvc
        ).go(plan.root());

        qry.addFragment(new RunningFragment<>(node, ectx));

        try {
            msgSrvc.send(
                    origNodeId,
                    FACTORY.queryStartResponse()
                            .queryId(qry.id())
                            .fragmentId(ectx.fragmentId())
                            .build()
            );
        } catch (IgniteInternalCheckedException e) {
            IgniteInternalException wrpEx = new IgniteInternalException("Failed to send reply. [nodeId=" + origNodeId + ']', e);

            throw wrpEx;
        }

        node.init();
    }

    private ResultSetMetadataInternal resultSetMetadata(PlanningContext ctx, RelDataType sqlType,
            @Nullable List<List<String>> origins) {
        return new ResultSetMetadataImpl(
                TypeUtils.getResultType(ctx.typeFactory(), ctx.catalogReader(), sqlType, origins),
                origins
        );
    }

    @SuppressWarnings("unchecked")
    private void onMessage(String nodeId, QueryStartRequest msg) {
        assert nodeId != null && msg != null;

        try {
            Query<RowT> qry = (Query<RowT>) queryRegistry.register(
                    new Query<>(
                            msg.queryId(),
                            nodeId,
                            null,
                            exchangeSrvc,
                            (q) -> queryRegistry.unregister(q.id()),
                            LOG
                    )
            );

            QueryPlan qryPlan = qryPlanCache.queryPlan(
                    new CacheKey(msg.schema(), msg.root()),
                    () -> prepareFragment(msg.root())
            );

            FragmentPlan plan = (FragmentPlan) qryPlan;

            final BaseQueryContext qctx = createQueryContext(Contexts.empty(), msg.schema());

            ExecutionContext<RowT> ectx = new ExecutionContext<>(
                    qctx,
                    taskExecutor,
                    msg.queryId(),
                    locNodeId,
                    nodeId,
                    msg.topologyVersion(),
                    msg.fragmentDescription(),
                    handler,
                    Commons.parametersMap(msg.parameters())
            );

            executeFragment(qry, plan, ectx);
        } catch (Throwable ex) {
            LOG.error("Failed to start query fragment", ex);

            mailboxRegistry.outboxes(msg.queryId(), msg.fragmentId(), -1)
                    .forEach(Outbox::close);
            mailboxRegistry.inboxes(msg.queryId(), msg.fragmentId(), -1)
                    .forEach(Inbox::close);

            try {
                msgSrvc.send(
                        nodeId,
                        FACTORY.queryStartResponse()
                                .queryId(msg.queryId())
                                .fragmentId(msg.fragmentId())
                                .error(ex)
                                .build()
                );
            } catch (Exception e) {
                LOG.error("Error occurred during send error message", e);

                IgniteInternalException wrpEx = new IgniteInternalException("Error occurred during send error message", e);

                e.addSuppressed(ex);

                RunningQuery qry = queryRegistry.query(msg.queryId());

                qry.cancel();

                throw wrpEx;
            }

            throw ex;
        }
    }

    private void onMessage(String nodeId, QueryStartResponse msg) {
        assert nodeId != null && msg != null;

        RunningQuery qry = queryRegistry.query(msg.queryId());

        if (qry != null) {
            assert qry instanceof RootQuery : "Unexpected query object: " + qry;

            ((RootQuery<?>) qry).onResponse(nodeId, msg.fragmentId(), msg.error());
        }
    }

    private void onMessage(String nodeId, ErrorMessage msg) {
        assert nodeId != null && msg != null;

        RunningQuery qry = queryRegistry.query(msg.queryId());

        if (qry != null && qry.state() != QueryState.CLOSED) {
            assert qry instanceof RootQuery : "Unexpected query object: " + qry;

            Exception e = new RemoteException(nodeId, msg.queryId(), msg.fragmentId(), msg.error());

            ((RootQuery<?>) qry).onError(e);
        }
    }

    private void onNodeLeft(ClusterNode node) {
        queryRegistry.runningQueries().forEach(query -> ((Query<?>) query).onNodeLeft(node.id()));
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        IgniteUtils.closeAll(iteratorsHolder::stop);
    }
}
