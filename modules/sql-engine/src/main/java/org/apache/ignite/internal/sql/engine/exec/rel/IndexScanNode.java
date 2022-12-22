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
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
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
public class IndexScanNode<RowT> extends StorageScanNode<RowT> {
    /** Schema index. */
    private final IgniteIndex schemaIndex;

    /** Index row layout. */
    private final BinaryTupleSchema indexRowSchema;

    private final RowHandler.RowFactory<RowT> factory;

    private final int[] parts;

    /** Participating columns. */
    private final @Nullable BitSet requiredColumns;

    private final @Nullable RangeIterable<RowT> rangeConditions;

    private final @Nullable Comparator<RowT> comp;

    private @Nullable Iterator<RangeCondition<RowT>> rangeConditionIterator;

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
        super(ctx, rowFactory, schemaTable, filters, rowTransformer, requiredColumns);

        assert !nullOrEmpty(parts);

        this.schemaIndex = schemaIndex;
        this.parts = parts;
        this.requiredColumns = requiredColumns;
        this.rangeConditions = rangeConditions;
        this.comp = comp;
        this.factory = rowFactory;

        rangeConditionIterator = rangeConditions == null ? null : rangeConditions.iterator();

        indexRowSchema = RowConverter.createIndexRowSchema(schemaIndex.columns(), schemaTable.descriptor());
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        super.rewindInternal();

        rangeConditionsProcessed = false;

        if (rangeConditions != null) {
            rangeConditionIterator = rangeConditions.iterator();
        }
    }

    @Override
    protected Publisher<RowT> scan() {
        if (!rangeConditionsProcessed) {
            RangeCondition<RowT> cond = null;

            if (rangeConditionIterator == null || !rangeConditionIterator.hasNext()) {
                rangeConditionsProcessed = true;
            } else {
                cond = rangeConditionIterator.next();

                rangeConditionsProcessed = !rangeConditionIterator.hasNext();
            }

            return indexPublisher(parts, cond);
        }

        return null;
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

        boolean roTx = context().transactionTime() != null;

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

            if (roTx) {
                pub = ((SortedIndex) schemaIndex.index()).scan(
                        part,
                        context().transactionTime(),
                        context().localNode(),
                        lower,
                        upper,
                        flags,
                        requiredColumns
                );
            } else {
                pub = ((SortedIndex) schemaIndex.index()).scan(
                        part,
                        context().transaction(),
                        lower,
                        upper,
                        flags,
                        requiredColumns
                );
            }
        } else {
            assert schemaIndex.type() == Type.HASH;
            assert cond != null && cond.lower() != null : "Invalid hash index condition.";

            BinaryTuple key = toBinaryTuple(cond.lower());

            if (roTx) {
                pub = schemaIndex.index().lookup(
                        part,
                        context().transactionTime(),
                        context().localNode(),
                        key,
                        requiredColumns
                );
            } else {
                pub = schemaIndex.index().lookup(
                        part,
                        context().transaction(),
                        key,
                        requiredColumns
                );
            }
        }

        return convertPublisher(pub);
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
}
