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

import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.util.IgniteMath;

/** Offset, fetch|limit support node. */
public class LimitNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {
    /** Offset param. */
    private final long offset;

    /** Fetch param. */
    private final long fetch;

    /** Fetch can be unset. */
    private final boolean fetchUndefined;

    /** Already processed (pushed to upstream) rows count. */
    private long rowsProcessed;

    /** Waiting results counter. */
    private int waiting;

    /** Upper requested rows. */
    private int requested;

    /**
     * Constructor.
     *
     * @param ctx     Execution context.
     */
    public LimitNode(
            ExecutionContext<RowT> ctx,
            long offset,
            long fetch
    ) {
        super(ctx);

        this.offset = offset;
        fetchUndefined = fetch == -1;
        this.fetch = fetch == -1 ? 0 : fetch;
    }

    /**
     * Several cases are need to be processed.
     *
     * <br><ol>
     * <li> request = 512, limit = 1, offset = not defined: need to pass 1 row and call {@link #end()}. </li>
     * <li> request = 512, limit = 512, offset = not defined: just need to pass all rows without {@link #end()} call. </li>
     * <li> request = 512, limit = 512, offset = 1: need to request initially 512 and further 1 row. </li>
     * </ol>
     */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0;

        if (fetchNone()) {
            end();

            return;
        }

        requested = rowsCnt;

        if (fetch > 0) {
            long remain = IgniteMath.addExact(fetch, offset) - rowsProcessed;

            rowsCnt = remain > rowsCnt ? rowsCnt : (int) remain;
        }

        waiting = rowsCnt;

        checkState();

        source().request(rowsCnt);
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        if (waiting == NOT_WAITING) {
            return;
        }

        ++rowsProcessed;

        --waiting;

        if (rowsProcessed > offset) {
            if (fetchUndefined || rowsProcessed <= fetch + offset) {
                // this two rows can`t be swapped, cause if all requested rows have been pushed it will trigger further request call.
                --requested;
                downstream().push(row);
            }
        }

        if (fetch > 0 && rowsProcessed == fetch + offset && requested > 0) {
            end();
        }

        if (waiting == 0 && requested > 0) {
            source().request(waiting = requested);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        if (waiting == NOT_WAITING) {
            return;
        }

        assert downstream() != null;

        waiting = NOT_WAITING;

        downstream().end();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        waiting = 0;
        requested = 0;
        rowsProcessed = 0;
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx != 0) {
            throw new IndexOutOfBoundsException();
        }

        return this;
    }

    /** {@code True} if requested 0 results, or all already processed. */
    private boolean fetchNone() {
        return (!fetchUndefined && fetch == 0) || (fetch > 0 && rowsProcessed == fetch + offset);
    }
}
