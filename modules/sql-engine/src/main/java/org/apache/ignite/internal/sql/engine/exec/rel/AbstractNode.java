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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.List;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Abstract node of execution tree.
 */
public abstract class AbstractNode<RowT> implements Node<RowT> {
    public static final int MODIFY_BATCH_SIZE = 100;

    protected static final int IO_BATCH_SIZE = Commons.IO_BATCH_SIZE;

    protected static final int IO_BATCH_CNT = Commons.IO_BATCH_COUNT;

    protected final int inBufSize = Commons.IN_BUFFER_SIZE;

    private final ExecutionContext<RowT> ctx;

    /** For debug purpose. */
    private volatile Thread thread;

    private Downstream<RowT> downstream;

    private boolean closed;

    private List<Node<RowT>> sources;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param ctx Execution context.
     */
    protected AbstractNode(ExecutionContext<RowT> ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override
    public ExecutionContext<RowT> context() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override
    public void register(List<Node<RowT>> sources) {
        this.sources = sources;

        for (int i = 0; i < sources.size(); i++) {
            sources.get(i).onRegister(requestDownstream(i));
        }
    }

    /** {@inheritDoc} */
    @Override
    public List<Node<RowT>> sources() {
        return sources;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        if (isClosed()) {
            return;
        }

        closeInternal();

        if (!nullOrEmpty(sources())) {
            sources().forEach(Commons::closeQuiet);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void rewind() {
        rewindInternal();

        if (!nullOrEmpty(sources())) {
            sources().forEach(Node::rewind);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onRegister(Downstream<RowT> downstream) {
        this.downstream = downstream;
    }

    /**
     * Processes given exception.
     *
     * @param e Exception.
     */
    public void onError(Throwable e) {
        Downstream<RowT> downstream = downstream();

        assert downstream != null;

        try {
            downstream.onError(e);
        } finally {
            Commons.closeQuiet(this);
        }
    }

    protected void closeInternal() {
        closed = true;
    }

    protected abstract void rewindInternal();

    /**
     * Get closed flag: {@code true} if the subtree is canceled.
     */
    public boolean isClosed() {
        return closed;
    }

    protected void checkState() throws Exception {
        if (context().isCancelled() || Thread.interrupted()) {
            throw new QueryCancelledException();
        }
        if (!IgniteUtils.assertionsEnabled()) {
            return;
        }
        if (thread == null) {
            thread = Thread.currentThread();
        } else {
            assert thread == Thread.currentThread() : format("expThread={}, actThread={}, "
                            + "qryId={}, fragmentId={}", thread.getName(), Thread.currentThread().getName(),
                    context().queryId(), context().fragmentId());
        }
    }

    protected abstract Downstream<RowT> requestDownstream(int idx);

    @Override
    public Downstream<RowT> downstream() {
        return downstream;
    }
}
