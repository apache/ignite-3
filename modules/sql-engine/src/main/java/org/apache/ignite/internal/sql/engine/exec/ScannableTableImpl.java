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
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER_OR_EQUAL;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS_OR_EQUAL;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.subscription.TransformingPublisher;
import org.apache.ignite.internal.utils.PrimaryReplica;
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
            RowFactory<RowT> rowFactory, @Nullable BitSet requiredColumns) {

        Publisher<BinaryRow> pub;
        TxAttributes txAttributes = ctx.txAttributes();

        int partId = partWithConsistencyToken.partId();

        if (txAttributes.readOnly()) {
            HybridTimestamp readTime = txAttributes.time();

            assert readTime != null;

            pub = internalTable.scan(partId, txAttributes.id(), readTime, ctx.localNode(),
                    txAttributes.coordinatorId());
        } else {
            PrimaryReplica recipient = new PrimaryReplica(ctx.localNode(), partWithConsistencyToken.enlistmentConsistencyToken());

            pub = internalTable.scan(
                    partId,
                    txAttributes.id(),
                    txAttributes.commitPartition(),
                    txAttributes.coordinatorId(),
                    recipient,
                    null,
                    null,
                    null,
                    0,
                    null
            );
        }

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
            @Nullable BitSet requiredColumns
    ) {
        TxAttributes txAttributes = ctx.txAttributes();
        RowHandler<RowT> handler = rowFactory.handler();

        Publisher<BinaryRow> pub;
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

            flags |= (cond.lowerInclude()) ? GREATER_OR_EQUAL : 0;
            flags |= (cond.upperInclude()) ? LESS_OR_EQUAL : 0;
        }

        int partId = partWithConsistencyToken.partId();

        if (txAttributes.readOnly()) {
            HybridTimestamp readTime = txAttributes.time();

            assert readTime != null;

            pub = internalTable.scan(
                    partId,
                    txAttributes.id(),
                    readTime,
                    ctx.localNode(),
                    indexId,
                    lower,
                    upper,
                    flags,
                    requiredColumns,
                    txAttributes.coordinatorId()
            );
        } else {
            pub = internalTable.scan(
                    partId,
                    txAttributes.id(),
                    txAttributes.commitPartition(),
                    txAttributes.coordinatorId(),
                    new PrimaryReplica(ctx.localNode(), partWithConsistencyToken.enlistmentConsistencyToken()),
                    indexId,
                    lower,
                    upper,
                    flags,
                    requiredColumns
            );
        }

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
            @Nullable BitSet requiredColumns
    ) {
        TxAttributes txAttributes = ctx.txAttributes();
        RowHandler<RowT> handler = rowFactory.handler();
        Publisher<BinaryRow> pub;

        BinaryTuple keyTuple = handler.toBinaryTuple(key);

        assert keyTuple.elementCount() == columns.size()
                : format("Key should contain exactly {} fields, but was {}", columns.size(), handler.toString(key));

        int partId = partWithConsistencyToken.partId();

        if (txAttributes.readOnly()) {
            HybridTimestamp readTime = txAttributes.time();

            assert readTime != null;

            pub = internalTable.lookup(
                    partId,
                    txAttributes.id(),
                    readTime,
                    ctx.localNode(),
                    indexId,
                    keyTuple,
                    null,
                    txAttributes.coordinatorId()
            );
        } else {
            pub = internalTable.lookup(
                    partId,
                    txAttributes.id(),
                    txAttributes.commitPartition(),
                    txAttributes.coordinatorId(),
                    new PrimaryReplica(ctx.localNode(), partWithConsistencyToken.enlistmentConsistencyToken()),
                    indexId,
                    keyTuple,
                    null
            );
        }

        TableRowConverter rowConverter = converterFactory.create(requiredColumns, partId);

        return new TransformingPublisher<>(pub, item -> rowConverter.toRow(ctx, item, rowFactory));
    }

    @Override
    public <RowT> CompletableFuture<RowT> primaryKeyLookup(
            ExecutionContext<RowT> ctx,
            @Nullable InternalTransaction explicitTx,
            RowFactory<RowT> rowFactory,
            RowT key,
            @Nullable BitSet requiredColumns
    ) {
        TableRowConverter converter = converterFactory.create(requiredColumns);

        BinaryRowEx keyRow = converter.toKeyRow(ctx, key);

        return internalTable.get(keyRow, explicitTx)
                .thenApply(tableRow -> {
                    if (tableRow == null) {
                        return null;
                    }

                    return converter.toRow(ctx, tableRow, rowFactory);
                });
    }

    private static <RowT> @Nullable BinaryTuplePrefix toBinaryTuplePrefix(
            int searchBoundSize,
            RowHandler<RowT> handler,
            @Nullable RowT prefix
    ) {
        if (prefix == null || handler.columnCount(prefix) == 0) {
            return null;
        }

        assert searchBoundSize >= handler.columnCount(prefix) : "Invalid range condition";

        return BinaryTuplePrefix.fromBinaryTuple(searchBoundSize, handler.toBinaryTuple(prefix));
    }
}
