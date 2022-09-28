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

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.schema.BinaryConverter;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * Scan node.
 * TODO: merge with {@link TableScanNode}
 */
public class IndexScanNode<RowT> extends AbstractNode<RowT> {
    /** Special value to highlights that all row were received and we are not waiting any more. */
    private static final int NOT_WAITING = -1;

    /** Index that provides access to underlying data. */
    private final SortedIndex physIndex;

    /** Table that is an object in SQL schema. */
    private final InternalIgniteTable schemaTable;

    private final RowHandler.RowFactory<RowT> factory;

    private final int[] parts;

    private final Queue<RowT> inBuff = new LinkedBlockingQueue<>(inBufSize);

    private final @Nullable Predicate<RowT> filters;

    private final @Nullable Function<RowT, RowT> rowTransformer;

    /** Participating columns. */
    private final @Nullable BitSet requiredColumns;

    private final @Nullable Supplier<RowT> lowerCond;

    private final @Nullable Supplier<RowT> upperCond;

    private final int flags;

    private int requested;

    private int waiting;

    private boolean inLoop;

    private Subscription activeSubscription;

    private int curPartIdx;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowType Output type of the current node.
     * @param schemaTable The table this node should scan.
     * @param parts Partition numbers to scan.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     * @param requiredColumns Optional set of column of interest.
     */
    public IndexScanNode(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteIndex index,
            InternalIgniteTable schemaTable,
            int[] parts,
            @Nullable Supplier<RowT> lowerCond,
            @Nullable Supplier<RowT> upperCond,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable BitSet requiredColumns
    ) {
        super(ctx, rowType);
        assert !nullOrEmpty(parts);

        this.physIndex = (SortedIndex) index.index();
        this.schemaTable = schemaTable;
        this.parts = parts;
        this.lowerCond = lowerCond;
        this.upperCond = upperCond;
        this.filters = filters;
        this.rowTransformer = rowTransformer;
        this.requiredColumns = requiredColumns;

        factory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        // TODO: create ticket to add flags support
        flags = SortedIndex.INCLUDE_LEFT & SortedIndex.INCLUDE_RIGHT;
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
            //TODO: Merge sorted iterators.
            physIndex.scan(
                    parts[curPartIdx++],
                    context().transaction(),
                    extractIndexColumns(lowerCond),
                    extractIndexColumns(upperCond),
                    flags,
                    requiredColumns
            ).subscribe(new SubscriberImpl());
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

    //TODO: Decouple from table and move code to converter.
    //TODO: Change table row layout -> index row layout.
    @Contract("null -> null")
    private @Nullable BinaryTuple extractIndexColumns(@Nullable Supplier<RowT> lowerCond) {
        if (lowerCond == null) {
            return null;
        }

        TableDescriptor desc = schemaTable.descriptor();

        BinaryTupleSchema binaryTupleSchema = BinaryTupleSchema.create(IntStream.range(0, desc.columnsCount())
                .mapToObj(i -> desc.columnDescriptor(i))
                .map(colDesc -> new Element(colDesc.physicalType(), colDesc.nullable()))
                .toArray(Element[]::new));

        BinaryRowEx binaryRowEx = schemaTable.toModifyRow(context(), lowerCond.get(), Operation.INSERT, null).getRow();

        ByteBuffer tuple = BinaryConverter.forRow(((Row) binaryRowEx).schema()).toTuple(binaryRowEx);

        return new BinaryTuple(binaryTupleSchema, tuple);
    }

    //TODO: Decouple from table and move code to converter.
    private RowT convert(BinaryTuple binTuple) {
        ExecutionContext<RowT> ectx = context();
        TableDescriptor desc = schemaTable.descriptor();

        RowHandler<RowT> handler = factory.handler();
        RowT res = factory.create();

        if (requiredColumns == null) {
            for (int i = 0; i < desc.columnsCount(); i++) {
                ColumnDescriptor colDesc = desc.columnDescriptor(i);

                handler.set(i, res, TypeUtils.toInternal(ectx, binTuple.value(colDesc.physicalIndex())));
            }
        } else {
            for (int i = 0, j = requiredColumns.nextSetBit(0); j != -1; j = requiredColumns.nextSetBit(j + 1), i++) {
                ColumnDescriptor colDesc = desc.columnDescriptor(j);

                handler.set(i, res, TypeUtils.toInternal(ectx, binTuple.value(colDesc.physicalIndex())));
            }
        }

        return res;
    }
}
