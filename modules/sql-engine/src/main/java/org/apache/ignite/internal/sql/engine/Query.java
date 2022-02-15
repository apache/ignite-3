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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionCancelledException;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;

/**
 * An object enclosing a state and a context of the query being executed on particular node.
 */
public class Query<RowT> implements RunningQuery {
    private final String initNodeId;

    private final UUID id;

    protected final Object mux = new Object();

    protected final Set<RunningFragment<RowT>> fragments;

    protected final QueryCancel cancel;

    protected final Consumer<Query<RowT>> unregister;

    protected volatile QueryState state = QueryState.INITED;

    protected final ExchangeService exchangeService;

    protected final IgniteLogger log;

    /** Creates the object. */
    public Query(
            UUID id,
            String initNodeId,
            QueryCancel cancel,
            ExchangeService exchangeService,
            Consumer<Query<RowT>> unregister,
            IgniteLogger log
    ) {
        this.id = id;
        this.unregister = unregister;
        this.initNodeId = initNodeId;
        this.exchangeService = exchangeService;
        this.log = log;

        this.cancel = cancel != null ? cancel : new QueryCancel();

        fragments = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public QueryState state() {
        return state;
    }

    protected void tryClose() {
        List<RunningFragment<RowT>> fragments = new ArrayList<>(this.fragments);

        AtomicInteger cntDown = new AtomicInteger(fragments.size());

        for (RunningFragment<RowT> frag : fragments) {
            frag.context().execute(() -> {
                frag.root().close();
                frag.context().cancel();

                if (cntDown.decrementAndGet() == 0) {
                    unregister.accept(this);
                }

            }, frag.root()::onError);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void cancel() {
        synchronized (mux) {
            if (state == QueryState.CLOSED) {
                return;
            }

            if (state == QueryState.INITED) {
                state = QueryState.CLOSING;

                try {
                    exchangeService.closeQuery(initNodeId, id);

                    return;
                } catch (IgniteInternalCheckedException e) {
                    log.warn("Cannot send cancel request to query initiator", e);
                }
            }

            if (state == QueryState.EXECUTING || state == QueryState.CLOSING) {
                state = QueryState.CLOSED;
            }
        }

        for (RunningFragment<RowT> frag : fragments) {
            frag.context().execute(() -> frag.root().onError(new ExecutionCancelledException()), frag.root()::onError);
        }

        tryClose();
    }

    /** Register a fragment within query. */
    public void addFragment(RunningFragment<RowT> f) {
        synchronized (mux) {
            if (state == QueryState.INITED) {
                state = QueryState.EXECUTING;
            }

            if (state == QueryState.CLOSING || state == QueryState.CLOSED) {
                throw new IgniteInternalException("The query was cancelled", new ExecutionCancelledException());
            }

            fragments.add(f);
        }
    }

    public boolean isCancelled() {
        return cancel.isCanceled();
    }

    /** A handler that should be called if any node left the cluster. */
    public void onNodeLeft(String nodeId) {
        if (initNodeId.equals(nodeId)) {
            cancel();
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(Query.class, this, "state", state, "fragments", fragments);
    }
}
