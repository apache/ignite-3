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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.PartitionProvider;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
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
    private final IgniteIndex schemaIndex;

    private final ScannableTable table;

    private final RowFactory<RowT> factory;

    /** Returns partitions to be used by this scan. */
    private final PartitionProvider<RowT> partitionProvider;

    /** Participating columns. */
    private final int @Nullable [] requiredColumns;

    private final @Nullable RangeIterable<RowT> rangeConditions;

    private final @Nullable Comparator<RowT> comp;

    private final List<String> columns;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowFactory Row factory.
     * @param tableDescriptor Table descriptor.
     * @param partitionProvider Partition provider.
     * @param comp Rows comparator.
     * @param rangeConditions Range conditions.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     * @param requiredColumns Optional set of column of interest.
     */
    public IndexScanNode(
            ExecutionContext<RowT> ctx,
            RowFactory<RowT> rowFactory,
            IgniteIndex schemaIndex,
            ScannableTable table,
            TableDescriptor tableDescriptor,
            PartitionProvider<RowT> partitionProvider,
            @Nullable Comparator<RowT> comp,
            @Nullable RangeIterable<RowT> rangeConditions,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable ImmutableIntList requiredColumns
    ) {
        super(ctx, filters, rowTransformer);

        this.schemaIndex = schemaIndex;
        this.table = table;
        this.partitionProvider = partitionProvider;
        this.requiredColumns = requiredColumns == null ? null : requiredColumns.toIntArray();
        this.rangeConditions = rangeConditions;
        this.comp = comp;
        this.factory = rowFactory;

        columns = schemaIndex.collation().getFieldCollations().stream()
                .map(RelFieldCollation::getFieldIndex)
                .map(tableDescriptor::columnDescriptor)
                .map(ColumnDescriptor::name)
                .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override
    protected Publisher<RowT> scan() {
        List<PartitionWithConsistencyToken> partitions = partitionProvider.getPartitions(context());

        if (rangeConditions != null) {
            return SubscriptionUtils.concat(
                    new TransformingIterator<>(rangeConditions.iterator(), cond -> indexPublisher(partitions, cond)));
        } else {
            return indexPublisher(partitions, null);
        }
    }

    private Publisher<RowT> indexPublisher(
            Collection<PartitionWithConsistencyToken> partsWithConsistencyTokens,
            @Nullable RangeCondition<RowT> cond
    ) {
        Iterator<Publisher<? extends RowT>> it = new TransformingIterator<>(
                partsWithConsistencyTokens.iterator(),
                partWithConsistencyToken -> partitionPublisher(partWithConsistencyToken, cond)
        );

        if (comp != null) {
            return SubscriptionUtils.orderedMerge(comp, Commons.SORTED_IDX_PART_PREFETCH_SIZE, it);
        } else {
            return SubscriptionUtils.concat(it);
        }
    }

    private Publisher<RowT> partitionPublisher(
            PartitionWithConsistencyToken partWithConsistencyToken,
            @Nullable RangeCondition<RowT> cond
    ) {
        int indexId = schemaIndex.id();
        ExecutionContext<RowT> ctx = context();

        switch (schemaIndex.type()) {
            case SORTED:
                return table.indexRangeScan(ctx, partWithConsistencyToken, factory, indexId,
                        columns, cond, requiredColumns);

            case HASH:
                return table.indexLookup(ctx, partWithConsistencyToken, factory, indexId,
                        columns, cond.lower(), requiredColumns);

            default:
                throw new AssertionError("Unexpected index type: " + schemaIndex.type());
        }
    }

    @Override
    protected void dumpMetrics0(IgniteStringBuilder writer) {
        super.dumpMetrics0(writer);
        writer.app(", indexName=").app(schemaIndex.name());
    }
}
