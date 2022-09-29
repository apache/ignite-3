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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.index.IndexDescriptor;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
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

    /** Index row schema. */
    private final BinaryTupleSchema indexRowSchema;

    /** Descriptor for table that index associated with. */
    private final TableDescriptor tableDescriptor;

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

        //TODO: fix HashIndex
        this.physIndex = (SortedIndex) index.index();
        tableDescriptor = schemaTable.descriptor();
        this.parts = parts;
        this.lowerCond = lowerCond;
        this.upperCond = upperCond;
        this.filters = filters;
        this.rowTransformer = rowTransformer;
        this.requiredColumns = requiredColumns;

        factory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        //TODO: Move to physIndex or IndexDesc ??
        indexRowSchema = buildIndexTupleSchema(physIndex.descriptor());

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
            //TODO: Introduce new publisher using merge-sort algo to merge partition index publishers.
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

    private BinaryTupleSchema buildIndexTupleSchema(IndexDescriptor indexDesc) {
        Element[] elements = indexDesc.columns().stream()
                .map(colName -> tableDescriptor.columnDescriptor(colName))
                .map(colDesc -> new Element(colDesc.physicalType(), colDesc.nullable()))
                .toArray(Element[]::new);

        return BinaryTupleSchema.create(elements);
    }

    //TODO: Decouple from table and move code to converter.
    //TODO: Change table row layout -> index row layout.
    @Contract("null -> null")
    private @Nullable BinaryTuple extractIndexColumns(@Nullable Supplier<RowT> conditionSupplier) {
        if (conditionSupplier == null) {
            return null;
        }

        RowT conditionRow = conditionSupplier.get();
        RowHandler<RowT> handler = factory.handler();

        int indexedColumnCnt = indexRowSchema.elementCount();
        int conditionColsCnt = handler.columnCount(conditionRow);

        BinaryTuplePrefixBuilder prefixBulder = new BinaryTuplePrefixBuilder(conditionColsCnt, indexedColumnCnt);

        for (int i = 0; i < conditionColsCnt; i++){
            Object val = handler.get(i, conditionRow);

            Element element = indexRowSchema.element(i);

            val = TypeUtils.fromInternal(context(), val, NativeTypeSpec.toClass(element.typeSpec(), element.nullable()));

            appendValue(prefixBulder, val);
        }

        return new BinaryTuple(indexRowSchema, prefixBulder.build());
    }

    //TODO: Decouple from table and move code to converter.
    private RowT convert(BinaryTuple binTuple) {
        ExecutionContext<RowT> ectx = context();

        RowHandler<RowT> handler = factory.handler();
        RowT res = factory.create();

        for (int i = 0; i < binTuple.count(); i++) {
            handler.set(i, res, TypeUtils.toInternal(ectx, binTuple.value(i)));
        }

        return res;
    }

    //TODO: Fixme. Copy-pasted from BinaryTupleRowSerializer test class.
    private BinaryTupleBuilder appendValue(BinaryTupleBuilder builder, Object value) {
        Element element = indexRowSchema.element(builder.elementIndex());

        if (value == null) {
            if (!element.nullable()) {
                throw new SchemaMismatchException("NULL value for non-nullable column in binary tuple builder.");
            }
            return builder.appendNull();
        }

        switch (element.typeSpec()) {
            case INT8:
                return builder.appendByte((byte) value);
            case INT16:
                return builder.appendShort((short) value);
            case INT32:
                return builder.appendInt((int) value);
            case INT64:
                return builder.appendLong((long) value);
            case FLOAT:
                return builder.appendFloat((float) value);
            case DOUBLE:
                return builder.appendDouble((double) value);
            case NUMBER:
                return builder.appendNumberNotNull((BigInteger) value);
            case DECIMAL:
                return builder.appendDecimalNotNull((BigDecimal) value, element.decimalScale());
            case UUID:
                return builder.appendUuidNotNull((UUID) value);
            case BYTES:
                return builder.appendBytesNotNull((byte[]) value);
            case STRING:
                return builder.appendStringNotNull((String) value);
            case BITMASK:
                return builder.appendBitmaskNotNull((BitSet) value);
            case DATE:
                return builder.appendDateNotNull((LocalDate) value);
            case TIME:
                return builder.appendTimeNotNull((LocalTime) value);
            case DATETIME:
                return builder.appendDateTimeNotNull((LocalDateTime) value);
            case TIMESTAMP:
                return builder.appendTimestampNotNull((Instant) value);
            default:
                break;
        }

        throw new InvalidTypeException("Unexpected type value: " + element.typeSpec());
    }
}
