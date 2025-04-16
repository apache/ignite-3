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
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
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
    private final long limit;

    /** Reverse-ordered rows in case of limited sort. */
    private ArrayDeque<RowT> reversed;

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
            long fetch) {
        super(ctx);

        assert fetch == -1 || fetch >= 0;
        assert offset >= 0;

        limit = fetch == -1 ? -1 : IgniteMath.addExact(fetch, offset);

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
        reversed = null;
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

        requested = rowsCnt;

        if (waiting == 0) {
            source().request(waiting = inBufSize);
        } else if (!inLoop) {
            this.execute(this::flush);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void push(List<RowT> batch) throws Exception {
        assert downstream() != null;
        assert waiting > 0;
        assert reversed == null || reversed.isEmpty();

        waiting -= batch.size();

        rows.addAll(batch);

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

    private void flush() throws Exception {
        assert waiting == NOT_WAITING;

        inLoop = true;
        try {
            // Prepare final order (reversed).
            if (limit > 0 && !rows.isEmpty()) {
                if (reversed == null) {
                    reversed = new ArrayDeque<>(rows.size());
                }

                int count = Math.min(rows.size(), inBufSize);
                for (int i = 0; i < count; i++) {
                    reversed.addFirst(rows.poll());
                }

                if (!rows.isEmpty()) {
                    // Allow the others to do their job.
                    this.execute(this::flush);

                    return;
                }
            }

            Queue<RowT> queue = reversed == null ? rows : reversed;

            if (requested > 0 && !queue.isEmpty()) {
                int batchSize = Math.min(queue.size(), requested);
                requested -= batchSize;

                List<RowT> batch = allocateBatch(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    batch.add(queue.poll());
                }

                downstream().push(batch);
                releaseBatch(batch);

                if (requested > 0 && !queue.isEmpty()) {
                    this.execute(this::flush);

                    return;
                }
            }

            if (requested > 0 && queue.isEmpty()) {
                requested = 0;

                downstream().end();
            }
        } finally {
            inLoop = false;
        }
    }
}
