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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableRow;
import org.jetbrains.annotations.Nullable;

/**
 * Scan node.
 */
public class TableScanNode<Row> extends AbstractNode<Row> {
    /** Special value to highlights that all row were received and we are not waiting any more. */
    private static final int NOT_WAITING = -1;

    /** */
    private final TableImpl table;

    /** */
    private final TableDescriptor desc;

    /** */
    private final RowHandler.RowFactory<Row> factory;

    /** */
    private final int[] parts;

    /** */
    private final Deque<Row> inBuff = new ArrayDeque<>(inBufSize);

    /** */
    private final @Nullable Predicate<Row> filters;

    /** */
    private final @Nullable Function<Row, Row> rowTransformer;

    /** Participating columns. */
    private final @Nullable ImmutableBitSet requiredColumns;

    /** */
    private int requested;

    /** */
    private int waiting;

    /** */
    private boolean inLoop;

    /** */
    private Subscription activeSubscription;

    /** */
    private int curPartIdx;

    /**
     * @param ctx Execution context.
     * @param rowType Output type of the current node.
     * @param desc Table descriptor this node should scan.
     * @param parts Partition numbers to scan.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     * @param requiredColumns Optional set of column of interest.
     */
    public TableScanNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        TableDescriptor desc,
        int[] parts,
        @Nullable Predicate<Row> filters,
        @Nullable Function<Row, Row> rowTransformer,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        super(ctx, rowType);

        table = desc.table();
        this.desc = desc;
        this.parts = parts;
        this.filters = filters;
        this.rowTransformer = rowTransformer;
        this.requiredColumns = requiredColumns;

        factory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert rowsCnt > 0 && requested == 0 : "rowsCnt=" + rowsCnt + ", requested=" + requested;

        checkState();

        requested = rowsCnt;

        if (!inLoop)
            context().execute(this::push, this::onError);
    }

    /** {@inheritDoc} */
    @Override public void closeInternal() {
        super.closeInternal();

        if (activeSubscription != null) {
            activeSubscription.cancel();

            activeSubscription = null;
        }
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        if (activeSubscription != null) {
            activeSubscription.cancel();

            activeSubscription = null;
        }
    }

    /** {@inheritDoc} */
    @Override public void register(List<Node<Row>> sources) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        throw new UnsupportedOperationException();
    }

    private void push() throws Exception {
        if (isClosed())
            return;

        checkState();

        if (waiting <= 0 && requested > 0 && !inBuff.isEmpty()) {
            inLoop = true;
            try {
                while (requested > 0 && !inBuff.isEmpty()) {
                    checkState();

                    Row row = inBuff.poll();

                    if (rowTransformer != null)
                        row = rowTransformer.apply(row);

                    requested--;
                    downstream().push(row);
                }
            }
            finally {
                inLoop = false;
            }
        }

        if (waiting == 0 || activeSubscription == null)
            requestNextBatch();

        if (waiting == NOT_WAITING && !inBuff.isEmpty())
            context().execute(this::push, this::onError);

        if (requested > 0 && waiting == NOT_WAITING && inBuff.isEmpty()) {
            requested = 0;
            downstream().end();
        }
    }

    private void requestNextBatch() {
        if (waiting == NOT_WAITING)
            return;

        if (waiting == 0)
            waiting = inBufSize;

        Subscription subscription = this.activeSubscription;
        if (subscription != null)
            subscription.request(waiting);
        else if (curPartIdx < parts.length)
            table.internalTable().scan(parts[curPartIdx++], null).subscribe(new SubscriberImpl(inBuff));
        else
            waiting = NOT_WAITING;
    }

    private class SubscriberImpl implements Flow.Subscriber<BinaryRow> {
        private final Deque<Row> inBuff;

        private SubscriberImpl(Deque<Row> buf) {
            inBuff = buf;
        }

        /** {@inheritDoc} */
        @Override public void onSubscribe(Subscription subscription) {
            assert TableScanNode.this.activeSubscription == null;

            TableScanNode.this.activeSubscription = subscription;
            subscription.request(inBufSize);
        }

        /** {@inheritDoc} */
        @Override public void onNext(BinaryRow binRow) {
            Row row = convert(binRow);

            if (filters != null && !filters.test(row))
                return;

            inBuff.add(row);

            if (inBuff.size() == inBufSize) {
                context().execute(() -> {
                    waiting = 0;
                    push();
                }, TableScanNode.this::onError);
            }
        }

        /** {@inheritDoc} */
        @Override public void onError(Throwable throwable) {
            context().execute(() -> {
                throw throwable;
            }, TableScanNode.this::onError);
        }

        /** {@inheritDoc} */
        @Override public void onComplete() {
            context().execute(() -> {
                activeSubscription = null;
                waiting -= inBuff.size();

                push();
            }, TableScanNode.this::onError);
        }
    }

    /** */
    private Row convert(BinaryRow binRow) {
        final org.apache.ignite.internal.schema.row.Row wrapped = table.schemaView().resolve(binRow);

        return desc.toRow(context(), TableRow.tuple(wrapped), factory, requiredColumns);
    }
}
