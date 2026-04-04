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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.util.IgniteMath;
import org.apache.ignite.internal.util.BoundedPriorityQueue;

/**
 * Sort node.
 */
public class SortNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {
    /** How many rows are requested by downstream. */
    private int requested;

    /** How many rows are we waiting for from the upstream. {@code -1} means end of stream. */
    private int waiting;

    private boolean inLoop;

    /** Rows buffer. */
    private final PriorityQueue<RowT> rows;

    /** SQL select limit. Negative if disabled. */
    private final long fetch;

    private final long offset;

    /** Reverse-ordered rows in case of limited sort. */
    private List<RowT> reversed;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param comp Rows comparator.
     * @param offset Offset.
     * @param fetch Limit.
     */
    public SortNode(ExecutionContext<RowT> ctx,
            Comparator<RowT> comp,
            long offset,
            long fetch
    ) {
        super(ctx);

        assert fetch == -1 || fetch >= 0;
        assert offset >= 0;

        // Offset must be set only together with fetch.
        // This is limitation is current implementation, and SortConverterRule
        // should not produce unsupported variant.
        if (offset > 0 && fetch == -1) {
            throw new AssertionError("Offset-only case is not supported by Sort node");
        }

        this.fetch = fetch;
        this.offset = offset;

        long limit = fetch == -1 ? -1 : IgniteMath.addExact(fetch, offset);

        if (limit < 1 || limit > Integer.MAX_VALUE) {
            rows = new PriorityQueue<>(comp);
        } else {
            rows = new BoundedPriorityQueue<>((int) limit, comp == null ? (Comparator<RowT>) Comparator.reverseOrder() : comp.reversed());
        }
    }

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param comp Rows comparator.
     */
    public SortNode(ExecutionContext<RowT> ctx, Comparator<RowT> comp) {
        this(ctx, comp, 0, -1);
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        rows.clear();
        if (reversed != null) {
            reversed.clear();
        }
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
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;
        assert waiting <= 0;

        onRequestReceived();

        if (fetch == 0) {
            downstream().end();

            return;
        }

        requested = rowsCnt;

        if (waiting == 0) {
            source().request(waiting = inBufSize);
        } else if (!inLoop) {
            this.execute(this::flush);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;
        assert reversed == null || reversed.isEmpty();

        onRowReceived();

        waiting--;

        rows.add(row);

        if (waiting == 0) {
            source().request(waiting = inBufSize);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        waiting = NOT_WAITING;

        flush();
    }

    @Override
    protected void dumpDebugInfo0(IgniteStringBuilder buf) {
        buf.app("class=").app(getClass().getSimpleName())
                .app(", requested=").app(requested)
                .app(", waiting=").app(waiting)
                .app(", fetch=").app(fetch)
                .app(", offset=").app(offset);
    }

    private void flush() throws Exception {
        assert waiting == NOT_WAITING;

        int processed = 0;

        inLoop = true;
        try {
            // Prepare final order (reversed).
            if (fetch > 0 && !rows.isEmpty()) {
                if (reversed == null) {
                    reversed = new ArrayList<>(rows.size());
                }

                while (rows.size() > offset) {
                    reversed.add(rows.poll());

                    if (++processed >= inBufSize) {
                        // Allow the others to do their job.
                        this.execute(this::flush);

                        return;
                    }
                }

                rows.clear();

                processed = 0;
            }

            while (requested > 0 && !(reversed == null ? rows.isEmpty() : reversed.isEmpty())) {
                requested--;

                downstream().push(reversed == null ? rows.poll() : reversed.remove(reversed.size() - 1));

                if (++processed >= inBufSize && requested > 0) {
                    // allow others to do their job
                    this.execute(this::flush);

                    return;
                }
            }

            if (reversed == null ? rows.isEmpty() : reversed.isEmpty()) {
                if (requested > 0) {
                    downstream().end();
                }

                requested = 0;
            }
        } finally {
            inLoop = false;
        }
    }
}
