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
import java.util.function.Supplier;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.util.BoundedPriorityQueue;
import org.jetbrains.annotations.Nullable;

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
    private final int limit;

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
            @Nullable Supplier<Integer> offset,
            @Nullable Supplier<Integer> fetch) {
        super(ctx);
        assert fetch == null || fetch.get() >= 0;
        assert offset == null || offset.get() >= 0;

        limit = fetch == null ? -1 : fetch.get() + (offset == null ? 0 : offset.get());

        if (limit < 1) {
            rows = new PriorityQueue<>(comp);
        } else {
            rows = new BoundedPriorityQueue<>(limit, comp == null ? (Comparator<RowT>) Comparator.reverseOrder() : comp.reversed());
        }
    }

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param comp Rows comparator.
     */
    public SortNode(ExecutionContext<RowT> ctx, Comparator<RowT> comp) {
        this(ctx, comp, null, null);
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

        checkState();

        requested = rowsCnt;

        if (waiting == 0) {
            source().request(waiting = inBufSize);
        } else if (!inLoop) {
            context().execute(this::flush, this::onError);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;
        assert reversed == null || reversed.isEmpty();

        checkState();

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

        checkState();

        waiting = -1;

        flush();
    }

    private void flush() throws Exception {
        if (isClosed()) {
            return;
        }

        assert waiting == -1;

        int processed = 0;

        inLoop = true;
        try {
            // Prepare final order (reversed).
            if (limit > 0 && !rows.isEmpty()) {
                if (reversed == null) {
                    reversed = new ArrayList<>(rows.size());
                }

                while (!rows.isEmpty()) {
                    reversed.add(rows.poll());

                    if (++processed >= inBufSize) {
                        // Allow the others to do their job.
                        context().execute(this::flush, this::onError);

                        return;
                    }
                }

                processed = 0;
            }

            while (requested > 0 && !(reversed == null ? rows.isEmpty() : reversed.isEmpty())) {
                checkState();

                requested--;

                downstream().push(reversed == null ? rows.poll() : reversed.remove(reversed.size() - 1));

                if (++processed >= inBufSize && requested > 0) {
                    // allow others to do their job
                    context().execute(this::flush, this::onError);

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
