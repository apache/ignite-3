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

import java.util.Comparator;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RuntimeHashIndex;
import org.apache.ignite.internal.sql.engine.exec.RuntimeIndex;
import org.apache.ignite.internal.sql.engine.exec.RuntimeSortedIndex;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;
import org.jetbrains.annotations.Nullable;

/**
 * Index spool node.
 */
public class IndexSpoolNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {
    /** Scan. */
    private final ScanNode<RowT> scan;

    /** Runtime index. */
    private final RuntimeIndex<RowT> idx;

    private int requested;

    private int waiting;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    private IndexSpoolNode(
            ExecutionContext<RowT> ctx,
            RuntimeIndex<RowT> idx,
            ScanNode<RowT> scan
    ) {
        super(ctx);

        this.idx = idx;
        this.scan = scan;
    }

    @Override
    public void onRegister(Downstream<RowT> downstream) {
        scan.onRegister(downstream);
    }

    @Override
    public Downstream<RowT> downstream() {
        return scan.downstream();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        scan.rewind();
    }

    /** {@inheritDoc} */
    @Override
    public void rewind() {
        rewindInternal();
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
        assert rowsCnt > 0;

        if (!indexReady()) {
            requested = rowsCnt;

            source().request(waiting = inBufSize);
        } else {
            scan.request(rowsCnt);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        idx.push(row);

        waiting--;

        if (waiting == 0) {
            source().request(waiting = inBufSize);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        waiting = NOT_WAITING;

        scan.request(requested);
    }

    /** {@inheritDoc} */
    @Override
    protected void closeInternal() {
        try {
            scan.close();
        } catch (Exception ex) {
            onError(ex);
        }

        try {
            idx.close();
        } catch (RuntimeException ex) {
            onError(ex);
        }

        super.closeInternal();
    }

    @Override
    protected void dumpDebugInfo0(IgniteStringBuilder buf) {
        buf.app("class=").app(getClass().getSimpleName())
                .app(", requested=").app(requested)
                .app(", waiting=").app(waiting);
    }

    private boolean indexReady() {
        return waiting == NOT_WAITING;
    }

    /**
     * CreateTreeSpool.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <RowT> IndexSpoolNode<RowT> createTreeSpool(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            RelCollation collation,
            Comparator<RowT> comp,
            Predicate<RowT> filter,
            RangeIterable<RowT> ranges
    ) {
        RuntimeSortedIndex<RowT> idx = new RuntimeSortedIndex<>(ctx, collation, comp);

        ScanNode<RowT> scan = new ScanNode<>(
                ctx,
                idx.scan(
                        rowType,
                        filter,
                        ranges
                )
        );

        return new IndexSpoolNode<>(ctx, idx, scan);
    }

    /**
     * CreateHashSpool.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <RowT> IndexSpoolNode<RowT> createHashSpool(
            ExecutionContext<RowT> ctx,
            ImmutableBitSet keys,
            @Nullable Predicate<RowT> filter,
            Supplier<RowT> searchRow,
            boolean allowNulls
    ) {
        RuntimeHashIndex<RowT> idx = new RuntimeHashIndex<>(ctx, keys, allowNulls);

        ScanNode<RowT> scan = new ScanNode<>(
                ctx,
                idx.scan(searchRow, filter)
        );

        return new IndexSpoolNode<>(ctx, idx, scan);
    }
}
