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
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;
import org.apache.ignite.internal.sql.engine.metadata.PartitionWithTerm;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchemaIndex;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.internal.util.TransformingIterator;
import org.jetbrains.annotations.Nullable;

/**
 * Execution node for index scan. Provide result of index scan by given index, partitions and range conditions.
 */
public class IndexScanNode<RowT> extends StorageScanNode<RowT> {
    /** Schema index. */
    private final IgniteSchemaIndex schemaIndex;

    private final ScannableTable table;

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
     * @param tableDescriptor Table descriptor.
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
            IgniteSchemaIndex schemaIndex,
            ScannableTable table,
            TableDescriptor tableDescriptor,
            Collection<PartitionWithTerm> partsWithTerms,
            @Nullable Comparator<RowT> comp,
            @Nullable RangeIterable<RowT> rangeConditions,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable BitSet requiredColumns
    ) {
        super(ctx, filters, rowTransformer);

        assert partsWithTerms != null && !partsWithTerms.isEmpty();

        this.schemaIndex = schemaIndex;
        this.table = table;
        this.partsWithTerms = partsWithTerms;
        this.requiredColumns = requiredColumns;
        this.rangeConditions = rangeConditions;
        this.comp = comp;
        this.factory = rowFactory;
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
        int indexId = schemaIndex.id();
        List<Integer> columns = schemaIndex.collation().getKeys();
        ExecutionContext<RowT> ctx = context();

        switch (schemaIndex.type()) {
            case SORTED:
                return table.indexRangeScan(ctx, partWithTerm, factory, indexId,
                        columns, cond, requiredColumns);

            case HASH:
                return table.indexLookup(ctx, partWithTerm, factory, indexId,
                        columns, cond.lower(), requiredColumns);

            default:
                throw new AssertionError("Unexpected index type: " + schemaIndex.type());
        }
    }
}
