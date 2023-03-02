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

import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
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
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;
import org.apache.ignite.internal.sql.engine.metadata.PartitionWithTerm;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.internal.util.TransformingIterator;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * Execution node for index scan. Provide result of index scan by given index, partitions and range conditions.
 */
public class IndexScanNode<RowT> extends StorageScanNode<RowT> {
    /** Schema index. */
    private final IgniteIndex schemaIndex;

    /** Index row layout. */
    private final BinaryTupleSchema indexRowSchema;

    private final RowHandler.RowFactory<RowT> factory;

    /** List of pairs containing the partition number to scan with the corresponding primary replica term. */
    private final Collection<PartitionWithTerm> partsWithTerms;

    /** Participating columns. */
    private final @Nullable BitSet requiredColumns;

    private final @Nullable RangeIterable<RowT> rangeConditions;

    private final @Nullable Comparator<RowT> comp;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowFactory Row factory.
     * @param schemaTable The table this node should scan.
     * @param partsWithTerms List of pairs containing the partition number to scan with the corresponding primary replica term.
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
            IgniteTable schemaTable,
            Collection<PartitionWithTerm> partsWithTerms,
            @Nullable Comparator<RowT> comp,
            @Nullable RangeIterable<RowT> rangeConditions,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable BitSet requiredColumns
    ) {
        super(ctx, rowFactory, schemaTable, filters, rowTransformer, requiredColumns);

        assert partsWithTerms != null && !partsWithTerms.isEmpty();
        assert rangeConditions == null || rangeConditions.size() > 0;

        this.schemaIndex = schemaIndex;
        this.partsWithTerms = partsWithTerms;
        this.requiredColumns = requiredColumns;
        this.rangeConditions = rangeConditions;
        this.comp = comp;
        this.factory = rowFactory;

        indexRowSchema = RowConverter.createIndexRowSchema(schemaIndex.columns(), schemaTable.descriptor());
    }

    /** {@inheritDoc} */
    @Override
    protected Publisher<RowT> scan() {
        if (rangeConditions != null) {
            return SubscriptionUtils.concat(
                    new TransformingIterator<>(rangeConditions.iterator(), cond -> indexPublisher(partsWithTerms, cond)));
        } else {
            return indexPublisher(partsWithTerms, null);
        }
    }

    private Publisher<RowT> indexPublisher(Collection<PartitionWithTerm> partsWithTerms, @Nullable RangeCondition<RowT> cond) {
        Iterator<Publisher<? extends RowT>> it = new TransformingIterator<>(
                partsWithTerms.iterator(),
                partWithTerm -> partitionPublisher(partWithTerm, cond)
        );

        if (comp != null) {
            return SubscriptionUtils.orderedMerge(comp, Commons.SORTED_IDX_PART_PREFETCH_SIZE, it);
        } else {
            return SubscriptionUtils.concat(it);
        }
    }

    private Publisher<RowT> partitionPublisher(PartitionWithTerm partWithTerm, @Nullable RangeCondition<RowT> cond) {
        Publisher<BinaryRow> pub;
        TxAttributes txAttributes = context().txAttributes();

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

            if (txAttributes.readOnly()) {
                pub = ((SortedIndex) schemaIndex.index()).scan(
                        partWithTerm.partId(),
                        txAttributes.time(),
                        context().localNode(),
                        lower,
                        upper,
                        flags,
                        requiredColumns
                );
            } else {
                pub = ((SortedIndex) schemaIndex.index()).scan(
                        partWithTerm.partId(),
                        txAttributes.id(),
                        new PrimaryReplica(context().localNode(), partWithTerm.term()),
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

            if (txAttributes.readOnly()) {
                pub = schemaIndex.index().lookup(
                        partWithTerm.partId(),
                        txAttributes.time(),
                        context().localNode(),
                        key,
                        requiredColumns
                );
            } else {
                pub = schemaIndex.index().lookup(
                        partWithTerm.partId(),
                        txAttributes.id(),
                        new PrimaryReplica(context().localNode(), partWithTerm.term()),
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
