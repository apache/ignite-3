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

import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;

import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowConverter;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.sql.engine.util.CompositePublisher;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * Scan node.
 * TODO: merge with {@link TableScanNode}
 */
public class IndexScanNode<RowT> extends AbstractNode<RowT> {
    /** Special value to highlights that all row were received and we are not waiting any more. */
    private static final int NOT_WAITING = -1;

    /** Schema index. */
    private final IgniteIndex schemaIndex;

    /** Index row layout. */
    private final BinaryTupleSchema indexRowSchema;

    private final RowHandler.RowFactory<RowT> factory;

    private final int[] parts;

    private final Queue<RowT> inBuff = new LinkedBlockingQueue<>(inBufSize);

    private final @Nullable Predicate<RowT> filters;

    private final @Nullable Function<RowT, RowT> rowTransformer;

    /** Participating columns. */
    private final @Nullable BitSet requiredColumns;

    private final RangeIterable<RowT> rangeConditions;

    private Iterator<RangeCondition<RowT>> rangeConditionIterator;

    private int requested;

    private int waiting;

    private boolean inLoop;

    private Subscription activeSubscription;

    private int curPartIdx;

    Comparator<BinaryTuple> cmp;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowType Output type of the current node.
     * @param schemaTable The table this node should scan.
     * @param parts Partition numbers to scan.
     * @param rangeConditions Range conditions.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     * @param requiredColumns Optional set of column of interest.
     */
    public IndexScanNode(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteIndex schemaIndex,
            InternalIgniteTable schemaTable,
            int[] parts,
            Comparator<RowT> cmp,
            @Nullable RangeIterable<RowT> rangeConditions,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable BitSet requiredColumns
    ) {
        super(ctx, rowType);
        assert !nullOrEmpty(parts);

        this.schemaIndex = schemaIndex;
        this.parts = parts;
        this.filters = filters;
        this.rowTransformer = rowTransformer;
        this.requiredColumns = requiredColumns;
        this.rangeConditions = rangeConditions;

        this.cmp = cmp == null ? null : (o1, o2) -> cmp.compare(convert(o1), convert(o2));

        // todo unified for hash index
        if (schemaIndex.type() == Type.SORTED) {
            assert cmp != null;

            rangeConditionIterator = rangeConditions == null ? null : rangeConditions.iterator();
        }

        factory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        indexRowSchema = RowConverter.createIndexRowSchema(schemaTable.descriptor(), schemaIndex.index().descriptor());
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert rowsCnt > 0 && requested == 0 : "rowsCnt=" + rowsCnt + ", requested=" + requested;

        checkState();

        requested = rowsCnt;

        if (!inLoop) {
            context().execute(this::push, this::onError);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void closeInternal() {
        super.closeInternal();

        if (activeSubscription != null) {
            activeSubscription.cancel();

            activeSubscription = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        curPartIdx = 0;
        rangeConditionIterator = null;

        if (activeSubscription != null) {
            activeSubscription.cancel();

            activeSubscription = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void register(List<Node<RowT>> sources) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        throw new UnsupportedOperationException();
    }

    private void push() throws Exception {
        if (isClosed()) {
            return;
        }

        checkState();

        if (requested > 0 && !inBuff.isEmpty()) {
            inLoop = true;
            try {
                while (requested > 0 && !inBuff.isEmpty()) {
                    checkState();

                    RowT row = inBuff.poll();

                    if (filters != null && !filters.test(row)) {
                        continue;
                    }

                    if (rowTransformer != null) {
                        row = rowTransformer.apply(row);
                    }

                    requested--;
                    downstream().push(row);
                }
            } finally {
                inLoop = false;
            }
        }

        if (waiting == 0 || activeSubscription == null) {
            requestNextBatch();
        }

        if (requested > 0 && waiting == NOT_WAITING) {
            if (inBuff.isEmpty()) {
                requested = 0;
                downstream().end();
            } else {
                context().execute(this::push, this::onError);
            }
        }
    }

    private void requestNextBatch() {
        if (waiting == NOT_WAITING) {
            return;
        }

        if (waiting == 0) {
            // we must not request rows more than inBufSize
            waiting = inBufSize - inBuff.size();
        }

        Subscription subscription = this.activeSubscription;
        if (subscription != null) {
            subscription.request(waiting);
        } else if (curPartIdx < parts.length) {
            if (schemaIndex.type() == Type.SORTED) {
                int flags = 0;
                BinaryTuple lowerBound = null;
                BinaryTuple upperBound = null;

                if (rangeConditionIterator == null) {
                    flags = SortedIndex.INCLUDE_LEFT | SortedIndex.INCLUDE_RIGHT;
                    curPartIdx = parts.length; // end marker
                } else {
                    RangeCondition<RowT> cond = rangeConditionIterator.next();

                    lowerBound = toBinaryTuplePrefix(cond.lower());
                    upperBound = toBinaryTuplePrefix(cond.upper());

                    flags |= (cond.lowerInclude()) ? SortedIndex.INCLUDE_LEFT : 0;
                    flags |= (cond.upperInclude()) ? SortedIndex.INCLUDE_RIGHT : 0;

                    if (!rangeConditionIterator.hasNext()) {
                        curPartIdx = parts.length; // end marker
                    }
                }

                CompositePublisher<BinaryTuple> compPublisher = new CompositePublisher<>(cmp);

                for (int p : parts) {
                    compPublisher.add(
                            ((SortedIndex) schemaIndex.index()).scan(
                                    p,
                                    context().transaction(),
                                    lowerBound,
                                    upperBound,
                                    flags,
                                    requiredColumns
                            ));
                }

                compPublisher.subscribe(new SubscriberImpl());
            } else {
                assert schemaIndex.type() == Type.HASH;

                int part = curPartIdx;
                BinaryTuple key = null;

                if (rangeConditions == null) {
                    curPartIdx++;
                } else {
                    if (rangeConditionIterator == null) {
                        rangeConditionIterator = rangeConditions.iterator();
                    }

                    RangeCondition<RowT> cond = rangeConditionIterator.next();

                    assert cond.lower() == cond.upper();

                    key = toBinaryTuple(cond.lower());

                    if (!rangeConditionIterator.hasNext()) { // Switch to next partition and reset range index.
                        rangeConditionIterator = null;
                        curPartIdx++;
                    }
                }

                schemaIndex.index().scan(
                        parts[part],
                        context().transaction(),
                        key,
                        requiredColumns
                ).subscribe(new SubscriberImpl());
            }
        } else {
            waiting = NOT_WAITING;
        }
    }

    private class SubscriberImpl implements Flow.Subscriber<BinaryTuple> {

        private int received = 0; // HB guarded here.

        /** {@inheritDoc} */
        @Override
        public void onSubscribe(Subscription subscription) {
            assert IndexScanNode.this.activeSubscription == null;

            IndexScanNode.this.activeSubscription = subscription;
            subscription.request(waiting);
        }

        /** {@inheritDoc} */
        @Override
        public void onNext(BinaryTuple binRow) {
            RowT row = convert(binRow);

            inBuff.add(row);

            if (++received == inBufSize) {
                received = 0;

                context().execute(() -> {
                    waiting = 0;
                    push();
                }, IndexScanNode.this::onError);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void onError(Throwable throwable) {
            context().execute(() -> {
                throw throwable;
            }, IndexScanNode.this::onError);
        }

        /** {@inheritDoc} */
        @Override
        public void onComplete() {
            context().execute(() -> {
                activeSubscription = null;
                waiting = 0;

                push();
            }, IndexScanNode.this::onError);
        }
    }

    @Contract("null -> null")
    private @Nullable BinaryTuple toBinaryTuplePrefix(@Nullable RowT condition) {
        if (condition == null) {
            return null;
        }

        return RowConverter.toBinaryTuplePrefix(context(), indexRowSchema, factory, condition);
    }

    @Contract("null -> null")
    private @Nullable BinaryTuple toBinaryTuple(@Nullable RowT condition) {
        if (condition == null) {
            return null;
        }

        return RowConverter.toBinaryTuple(context(), indexRowSchema, factory, condition);
    }

    private RowT convert(BinaryTuple binaryTuple) {
        return RowConverter.toRow(context(), binaryTuple, factory);
    }
}
