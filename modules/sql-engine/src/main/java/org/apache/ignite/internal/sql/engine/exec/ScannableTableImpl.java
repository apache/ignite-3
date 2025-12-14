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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER_OR_EQUAL;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS_OR_EQUAL;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.table.IndexScanCriteria;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.OperationContext;
import org.apache.ignite.internal.table.TxContext;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.subscription.TransformingPublisher;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ScannableTable} that uses {@link InternalTable}.
 */
public class ScannableTableImpl implements ScannableTable {

    private final InternalTable internalTable;

    private final TableRowConverterFactory converterFactory;

    /** Constructor. */
    public ScannableTableImpl(InternalTable internalTable, TableRowConverterFactory converterFactory) {
        this.internalTable = internalTable;
        this.converterFactory = converterFactory;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> Publisher<RowT> scan(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
            RowFactory<RowT> rowFactory, int @Nullable [] requiredColumns) {
        TxContext txContext = transactionalContextFrom(ctx.txAttributes(), partWithConsistencyToken.enlistmentConsistencyToken());

        int partId = partWithConsistencyToken.partId();

        Publisher<BinaryRow> pub = internalTable.scan(
                partId,
                ctx.localNode(),
                OperationContext.create(txContext)
        );

        TableRowConverter rowConverter = converterFactory.create(requiredColumns, partId);

        return new TransformingPublisher<>(pub, item -> rowConverter.toRow(ctx, item, rowFactory));
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> Publisher<RowT> indexRangeScan(
            ExecutionContext<RowT> ctx,
            PartitionWithConsistencyToken partWithConsistencyToken,
            RowFactory<RowT> rowFactory,
            int indexId,
            List<String> columns,
            @Nullable RangeCondition<RowT> cond,
            int @Nullable [] requiredColumns
    ) {
        TxContext txContext = transactionalContextFrom(ctx.txAttributes(), partWithConsistencyToken.enlistmentConsistencyToken());

        RowHandler<RowT> handler = ctx.rowAccessor();

        BinaryTuplePrefix lower;
        BinaryTuplePrefix upper;

        int flags = 0;

        if (cond == null) {
            flags = LESS_OR_EQUAL | GREATER_OR_EQUAL;
            lower = null;
            upper = null;
        } else {
            lower = toBinaryTuplePrefix(columns.size(), handler, cond.lower());
            upper = toBinaryTuplePrefix(columns.size(), handler, cond.upper());

            flags |= (cond.lowerInclude()) ? GREATER_OR_EQUAL : GREATER;
            flags |= (cond.upperInclude()) ? LESS_OR_EQUAL : LESS;
        }

        int partId = partWithConsistencyToken.partId();

        Publisher<BinaryRow> pub = internalTable.scan(
                partId,
                ctx.localNode(),
                indexId,
                IndexScanCriteria.range(lower, upper, flags),
                OperationContext.create(txContext)
        );

        TableRowConverter rowConverter = converterFactory.create(requiredColumns, partId);

        return new TransformingPublisher<>(pub, item -> rowConverter.toRow(ctx, item, rowFactory));
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> Publisher<RowT> indexLookup(
            ExecutionContext<RowT> ctx,
            PartitionWithConsistencyToken partWithConsistencyToken,
            RowFactory<RowT> rowFactory,
            int indexId,
            List<String> columns,
            RowT key,
            int @Nullable [] requiredColumns
    ) {
        TxContext txContext = transactionalContextFrom(ctx.txAttributes(), partWithConsistencyToken.enlistmentConsistencyToken());

        RowHandler<RowT> handler = ctx.rowAccessor();

        BinaryTuple keyTuple = handler.toBinaryTuple(key);

        assert keyTuple.elementCount() == columns.size()
                : format("Key should contain exactly {} fields, but was {}", columns.size(), handler.toString(key));

        int partId = partWithConsistencyToken.partId();

        Publisher<BinaryRow> pub = internalTable.scan(
                partId,
                ctx.localNode(),
                indexId,
                IndexScanCriteria.lookup(keyTuple),
                OperationContext.create(txContext)
        );

        TableRowConverter rowConverter = converterFactory.create(requiredColumns, partId);

        return new TransformingPublisher<>(pub, item -> rowConverter.toRow(ctx, item, rowFactory));
    }

    @Override
    public <RowT> CompletableFuture<RowT> primaryKeyLookup(
            ExecutionContext<RowT> ctx,
            InternalTransaction tx,
            RowFactory<RowT> rowFactory,
            RowT key,
            int @Nullable [] requiredColumns
    ) {
        assert tx != null;

        TableRowConverter converter = converterFactory.create(requiredColumns);

        BinaryRowEx keyRow = converter.toKeyRow(ctx, key);

        return internalTable.get(keyRow, tx)
                .thenApply(tableRow -> {
                    if (tableRow == null) {
                        return null;
                    }

                    return converter.toRow(ctx, tableRow, rowFactory);
                });
    }

    @Override
    public CompletableFuture<Long> estimatedSize() {
        return internalTable.estimatedSize();
    }

    private static <RowT> @Nullable BinaryTuplePrefix toBinaryTuplePrefix(
            int searchBoundSize,
            RowHandler<RowT> handler,
            @Nullable RowT prefix
    ) {
        if (prefix == null) {
            return null;
        }

        int columnsCount = handler.columnsCount(prefix);

        if (columnsCount == 0) {
            return null;
        }

        if (searchBoundSize < columnsCount) {
            throw new IllegalStateException("Invalid range condition");
        }

        return BinaryTuplePrefix.fromBinaryTuple(searchBoundSize, handler.toBinaryTuple(prefix));
    }

    private static TxContext transactionalContextFrom(TxAttributes txAttributes, long enlistmentConsistencyToken) {
        if (txAttributes.readOnly()) {
            HybridTimestamp timestamp = txAttributes.time();

            assert timestamp != null;

            return TxContext.readOnly(txAttributes.id(), txAttributes.coordinatorId(), timestamp);
        }

        ZonePartitionId commitPartition = txAttributes.commitPartition();

        assert commitPartition != null;

        return TxContext.readWrite(txAttributes.id(), txAttributes.coordinatorId(), commitPartition, enlistmentConsistencyToken);
    }
}
