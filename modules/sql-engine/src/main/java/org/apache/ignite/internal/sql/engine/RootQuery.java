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
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.CancelFlag;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.rel.Node;
import org.apache.ignite.internal.sql.engine.exec.rel.RootNode;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;

/**
 * The RootQuery is created on the query initiator (originator) node as the first step of a query run; It contains the information about
 * query state, contexts, remote fragments; It provides 'cancel' functionality for running query like a base query class.
 */
public class RootQuery<RowT> extends Query<RowT> {
    /** SQL query. */
    private final String sql;

    /** Parameters. */
    private final Object[] params;

    private final Set<String> remotes;

    private final Set<RemoteFragmentKey> waiting;

    private volatile RootNode<RowT> root;

    private volatile PlanningContext pctx;

    private final BaseQueryContext ctx;

    /** Creates the object. */
    public RootQuery(
            String sql,
            SchemaPlus schema,
            Object[] params,
            ExchangeService exch,
            Consumer<Query<RowT>> unregister,
            IgniteLogger log
    ) {
        this(sql, schema, params, new QueryCancel(), exch, unregister, log);
    }

    /** Creates the object. */
    public RootQuery(
            String sql,
            SchemaPlus schema,
            Object[] params,
            QueryCancel cancel,
            ExchangeService exch,
            Consumer<Query<RowT>> unregister,
            IgniteLogger log
    ) {
        super(
                UUID.randomUUID(),
                null,
                cancel,
                exch,
                unregister,
                log
        );

        this.sql = sql;
        this.params = params;

        remotes = new HashSet<>();
        waiting = new HashSet<>();

        ctx = BaseQueryContext.builder()
                .cancel(cancel)
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(schema)
                                .build()
                )
                .logger(log)
                .build();
    }

    /**
     * Creates the new root that inherits the query parameters from {@code this} query. Is used to execute DML query immediately after
     * (inside) DDL. e.g.: CREATE TABLE MY_TABLE AS SELECT ... FROM ...;
     *
     * @param schema new schema.
     */
    public RootQuery<RowT> childQuery(SchemaPlus schema) {
        return new RootQuery<>(sql, schema, params, cancel, exchangeService, unregister, log);
    }

    public BaseQueryContext context() {
        return ctx;
    }

    public String sql() {
        return sql;
    }

    public Object[] parameters() {
        return params;
    }

    /**
     * Starts mapping phase for the query.
     */
    public void mapping() {
        synchronized (mux) {
            if (state == QueryState.CLOSED) {
                throw new IgniteInternalException("The query was cancelled while executing.");
            }

            state = QueryState.MAPPING;
        }
    }

    /**
     * Starts execution phase for the query and setup remote fragments.
     */
    public void run(ExecutionContext<RowT> ctx, MultiStepPlan plan, Node<RowT> root) {
        synchronized (mux) {
            if (state == QueryState.CLOSED) {
                throw new IgniteInternalException("The query was cancelled while executing.");
            }

            RootNode<RowT> rootNode = new RootNode<>(ctx, plan.metadata().rowType(), this::tryClose);
            rootNode.register(root);

            addFragment(new RunningFragment<>(rootNode, ctx));

            this.root = rootNode;

            for (int i = 1; i < plan.fragments().size(); i++) {
                Fragment fragment = plan.fragments().get(i);
                List<String> nodes = plan.mapping(fragment).nodeIds();

                remotes.addAll(nodes);

                for (String node : nodes) {
                    waiting.add(new RemoteFragmentKey(node, fragment.fragmentId()));
                }
            }

            state = QueryState.EXECUTING;
        }
    }

    /**
     * Can be called multiple times after receive each error at {@link #onResponse(RemoteFragmentKey, Throwable)}.
     */
    @Override
    protected void tryClose() {
        QueryState state0 = null;

        synchronized (mux) {
            if (state == QueryState.CLOSED) {
                return;
            }

            if (state == QueryState.INITED || state == QueryState.PLANNING || state == QueryState.MAPPING) {
                state = QueryState.CLOSED;

                return;
            }

            if (state == QueryState.EXECUTING) {
                state0 = state = QueryState.CLOSING;

                root.closeInternal();
            }

            if (state == QueryState.CLOSING && waiting.isEmpty()) {
                state0 = state = QueryState.CLOSED;
            }
        }

        if (state0 == QueryState.CLOSED) {
            try {
                IgniteInternalException wrpEx = null;

                for (String nodeId : remotes) {
                    try {
                        if (!nodeId.equals(root.context().localNodeId())) {
                            exchangeService.closeQuery(nodeId, id());
                        }
                    } catch (IgniteInternalCheckedException e) {
                        if (wrpEx == null) {
                            wrpEx = new IgniteInternalException("Failed to send cancel message. [nodeId=" + nodeId + ']', e);
                        } else {
                            wrpEx.addSuppressed(e);
                        }
                    }
                }

                if (wrpEx != null) {
                    log.warn("An exception occures during the query cancel", wrpEx);
                }
            } finally {
                super.tryClose();
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void cancel() {
        cancel.cancel();

        tryClose();
    }

    /** Returns a planning context to prepare this query. */
    public PlanningContext planningContext() {
        synchronized (mux) {
            if (state == QueryState.CLOSED || state == QueryState.CLOSING) {
                throw new IgniteInternalException("The query was cancelled while executing.");
            }

            if (state == QueryState.EXECUTING || state == QueryState.MAPPING) {
                throw new IgniteInternalException("Invalid query flow");
            }

            if (pctx == null) {
                state = QueryState.PLANNING;

                pctx = PlanningContext.builder()
                        .parentContext(ctx)
                        .query(sql)
                        .parameters(params)
                        .build();

                try {
                    cancel.add(() -> pctx.unwrap(CancelFlag.class).requestCancel());
                } catch (QueryCancelledException e) {
                    throw new IgniteInternalException(e.getMessage(), e);
                }
            }

            return pctx;
        }
    }

    public Iterator<RowT> iterator() {
        return root;
    }

    /** {@inheritDoc} */
    @Override
    public void onNodeLeft(String nodeId) {
        List<RemoteFragmentKey> fragments;

        synchronized (mux) {
            fragments = waiting.stream().filter(f -> f.nodeId().equals(nodeId)).collect(Collectors.toList());
        }

        if (!nullOrEmpty(fragments)) {
            IgniteInternalException ex = new IgniteInternalException(
                    "Failed to start query, node left. nodeId=" + nodeId);

            for (RemoteFragmentKey fragment : fragments) {
                onResponse(fragment, ex);
            }
        }
    }

    public void onResponse(String nodeId, long fragmentId, Throwable error) {
        onResponse(new RemoteFragmentKey(nodeId, fragmentId), error);
    }

    private void onResponse(RemoteFragmentKey fragment, Throwable error) {
        QueryState state;
        synchronized (mux) {
            waiting.remove(fragment);

            state = this.state;
        }

        if (error != null) {
            onError(error);
        } else if (state == QueryState.CLOSING) {
            tryClose();
        }
    }

    /**
     * The handler that should be called if any error occurs during the query execution.
     *
     * @param error The occurred error.
     */
    public void onError(Throwable error) {
        root.onError(error);

        tryClose();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(RootQuery.class, this);
    }
}
