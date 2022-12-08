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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowConverter;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.CompositePublisher;
import org.apache.ignite.internal.sql.engine.util.SortingCompositePublisher;
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

    private final Function<BinaryRow, RowT> tableRowConverter;

    /** Participating columns. */
    private final @Nullable BitSet requiredColumns;

    private final @Nullable RangeIterable<RowT> rangeConditions;

    private final @Nullable Comparator<RowT> comp;

    private @Nullable Iterator<RangeCondition<RowT>> rangeConditionIterator;

    private int requested;

    private int waiting;

    private boolean inLoop;

    private @Nullable Subscription activeSubscription;

    private boolean rangeConditionsProcessed;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowFactory Row factory.
     * @param schemaTable The table this node should scan.
     * @param parts Partition numbers to scan.
     * @param comp Rows comparator.
     * @param rangeConditions Range conditions.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     * @param requiredColumns Optional set of column of interest.
     */
    public IndexScanNode(
            ExecutionContext<RowT> ctx,
            RowHandler.RowFactory<RowT> rowFactory,
            IgniteIndex schemaIndex,
            InternalIgniteTable schemaTable,
            int[] parts,
            @Nullable Comparator<RowT> comp,
            @Nullable RangeIterable<RowT> rangeConditions,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable BitSet requiredColumns
    ) {
        super(ctx);

        assert !nullOrEmpty(parts);

        assert ctx.transaction() != null || ctx.transactionTime() != null : "Transaction not initialized.";

        this.schemaIndex = schemaIndex;
        this.parts = parts;
        this.filters = filters;
        this.rowTransformer = rowTransformer;
        this.requiredColumns = requiredColumns;
        this.rangeConditions = rangeConditions;
        this.comp = comp;
        this.factory = rowFactory;

        rangeConditionIterator = rangeConditions == null ? null : rangeConditions.iterator();

        tableRowConverter = row -> schemaTable.toRow(context(), row, factory, requiredColumns);

        indexRowSchema = RowConverter.createIndexRowSchema(schemaIndex.columns(), schemaTable.descriptor());
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
        rangeConditionsProcessed = false;

        if (rangeConditions != null) {
            rangeConditionIterator = rangeConditions.iterator();
        }

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

        if (requested > 0) {
            if (waiting == 0 || activeSubscription == null) {
                requestNextBatch();
            }
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
        } else if (!rangeConditionsProcessed) {
            RangeCondition<RowT> cond = null;

            if (rangeConditionIterator == null || !rangeConditionIterator.hasNext()) {
                rangeConditionsProcessed = true;
            } else {
                cond = rangeConditionIterator.next();

                rangeConditionsProcessed = !rangeConditionIterator.hasNext();
            }

            indexPublisher(parts, cond).subscribe(new SubscriberImpl());
        } else {
            waiting = NOT_WAITING;
        }
    }

    private Publisher<RowT> indexPublisher(int[] parts, @Nullable RangeCondition<RowT> cond) {
        List<Flow.Publisher<RowT>> partPublishers = new ArrayList<>(parts.length);

        for (int p : parts) {
            partPublishers.add(partitionPublisher(p, cond));
        }

        return comp != null
                ? new SortingCompositePublisher<>(partPublishers, comp, Commons.SORTED_IDX_PART_PREFETCH_SIZE)
                : new CompositePublisher<>(partPublishers);
    }

    private Flow.Publisher<RowT> partitionPublisher(int part, @Nullable RangeCondition<RowT> cond) {
        Publisher<BinaryRow> pub;

        if (schemaIndex.type() == Type.SORTED) {
            int flags = 0;
            BinaryTuplePrefix lower = null;
            BinaryTuplePrefix upper = null;

            if (cond == null) {
                flags = SortedIndex.INCLUDE_LEFT | SortedIndex.INCLUDE_RIGHT;
            } else {
                lower = toBinaryTuplePrefix(cond.lower());
                upper = toBinaryTuplePrefix(cond.upper());

                flags |= (cond.lowerInclude()) ? SortedIndex.INCLUDE_LEFT : 0;
                flags |= (cond.upperInclude()) ? SortedIndex.INCLUDE_RIGHT : 0;
            }

            if (context().transactionTime() != null) {
                pub = ((SortedIndex) schemaIndex.index()).scan(
                        part,
                        context().transactionTime(),
                        context().localNode(),
                        lower,
                        upper,
                        flags,
                        requiredColumns);
            } else {
                pub = ((SortedIndex) schemaIndex.index()).scan(
                        part,
                        context().transaction(),
                        lower,
                        upper,
                        flags,
                        requiredColumns);
            }
        } else {
            assert schemaIndex.type() == Type.HASH;
            assert cond != null && cond.lower() != null : "Invalid hash index condition.";

            BinaryTuple key = toBinaryTuple(cond.lower());

            pub = schemaIndex.index().lookup(
                    part,
                    context().transaction(),
                    key,
                    requiredColumns
            );
        }

        return downstream -> {
            // BinaryRow -> RowT converter.
            Subscriber<BinaryRow> subs = new Subscriber<>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    downstream.onSubscribe(subscription);
                }

                @Override
                public void onNext(BinaryRow item) {
                    downstream.onNext(convert(item));
                }

                @Override
                public void onError(Throwable throwable) {
                    downstream.onError(throwable);
                }

                @Override
                public void onComplete() {
                    downstream.onComplete();
                }
            };

            pub.subscribe(subs);
        };
    }

    private class SubscriberImpl implements Flow.Subscriber<RowT> {
        /** {@inheritDoc} */
        @Override
        public void onSubscribe(Subscription subscription) {
            assert IndexScanNode.this.activeSubscription == null;

            IndexScanNode.this.activeSubscription = subscription;
            subscription.request(waiting);
        }

        /** {@inheritDoc} */
        @Override
        public void onNext(RowT row) {
            inBuff.add(row);

            if (inBuff.size() == inBufSize) {
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
    private @Nullable BinaryTuplePrefix toBinaryTuplePrefix(@Nullable RowT condition) {
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

    private RowT convert(BinaryRow binaryRow) {
        return tableRowConverter.apply(binaryRow);
    }
}
