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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Predicate;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;

/**
 * FilterNode.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class FilterNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {
    private final Predicate<RowT> pred;

    private final Deque<RowT> inBuf = new ArrayDeque<>(inBufSize);

    private int requested;

    private int waiting;

    private boolean inLoop;

    // Metrics
    private long filteredRows;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param ctx  Execution context.
     * @param pred Predicate.
     */
    public FilterNode(ExecutionContext<RowT> ctx, Predicate<RowT> pred) {
        super(ctx);

        this.pred = pred;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;

        onRequestReceived();

        requested = rowsCnt;

        if (!inLoop) {
            this.execute(this::filter);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        onRowReceived();

        waiting--;

        if (pred.test(row)) {
            inBuf.add(row);
        } else {
            onRowFiltered();
        }

        filter();
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        waiting = NOT_WAITING;

        filter();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx != 0) {
            throw new IndexOutOfBoundsException();
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        inBuf.clear();
    }

    private void filter() throws Exception {
        inLoop = true;
        try {
            int processed = 0;
            while (requested > 0 && !inBuf.isEmpty()) {
                requested--;
                downstream().push(inBuf.remove());

                if (processed++ >= inBufSize) {
                    // Allow others to do their job.
                    execute(this::filter);

                    break;
                }
            }
        } finally {
            inLoop = false;
        }

        if (inBuf.isEmpty() && waiting == 0) {
            source().request(waiting = inBufSize);
        }

        if (waiting == NOT_WAITING && requested > 0) {
            assert inBuf.isEmpty();

            requested = 0;
            downstream().end();
        }
    }

    @Override
    protected void dumpDebugInfo0(IgniteStringBuilder buf) {
        buf.app("class=").app(getClass().getSimpleName())
                .app(", requested=").app(requested)
                .app(", waiting=").app(waiting);
    }

    @Override
    protected void dumpMetrics0(IgniteStringBuilder writer) {
        super.dumpMetrics0(writer);
        writer.app(", filteredRows=").app(filteredRows);
    }

    private void onRowFiltered() {
        filteredRows++;
    }
}
