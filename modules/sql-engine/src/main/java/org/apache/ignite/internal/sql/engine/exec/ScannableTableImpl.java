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

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.metadata.PartitionWithTerm;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.util.subscription.TransformingPublisher;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ScannableTable} that uses {@link InternalTable}.
 */
public class ScannableTableImpl implements ScannableTable {

    private final InternalTable internalTable;

    private final TableRowConverter rowConverter;

    private final TableDescriptor tableDescriptor;

    /** Constructor. */
    public ScannableTableImpl(InternalTable internalTable, TableRowConverter rowConverter, TableDescriptor tableDescriptor) {
        this.internalTable = internalTable;
        this.rowConverter = rowConverter;
        this.tableDescriptor = tableDescriptor;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> Publisher<RowT> scan(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
            RowFactory<RowT> rowFactory, @Nullable BitSet requiredColumns) {

        Publisher<BinaryRow> pub;
        TxAttributes txAttributes = ctx.txAttributes();

        if (txAttributes.readOnly()) {
            HybridTimestamp readTime = txAttributes.time();

            assert readTime != null;

            pub = internalTable.scan(partWithTerm.partId(), readTime, ctx.localNode());
        } else {
            PrimaryReplica recipient = new PrimaryReplica(ctx.localNode(), partWithTerm.term());

            pub = internalTable.scan(partWithTerm.partId(), txAttributes.id(), recipient, null, null, null, 0, null);
        }

        return new TransformingPublisher<>(pub, item -> rowConverter.toRow(ctx, item, rowFactory, requiredColumns));
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> Publisher<RowT> indexRangeScan(
            ExecutionContext<RowT> ctx,
            PartitionWithTerm partWithTerm,
            RowFactory<RowT> rowFactory,
            int indexId,
            List<Integer> columns,
            @Nullable RangeCondition<RowT> cond,
            @Nullable BitSet requiredColumns
    ) {

        BinaryTupleSchema indexRowSchema = RowConverter.createIndexRowSchema(columns, tableDescriptor);
        TxAttributes txAttributes = ctx.txAttributes();

        Publisher<BinaryRow> pub;
        BinaryTuplePrefix lower;
        BinaryTuplePrefix upper;

        int flags = 0;

        if (cond == null) {
            flags = SortedIndex.INCLUDE_LEFT | SortedIndex.INCLUDE_RIGHT;
            lower = null;
            upper = null;
        } else {
            lower = toBinaryTuplePrefix(ctx, indexRowSchema, cond.lower(), rowFactory);
            upper = toBinaryTuplePrefix(ctx, indexRowSchema, cond.upper(), rowFactory);

            flags |= (cond.lowerInclude()) ? SortedIndex.INCLUDE_LEFT : 0;
            flags |= (cond.upperInclude()) ? SortedIndex.INCLUDE_RIGHT : 0;
        }

        if (txAttributes.readOnly()) {
            pub = internalTable.scan(
                    partWithTerm.partId(),
                    txAttributes.time(),
                    ctx.localNode(),
                    indexId,
                    lower,
                    upper,
                    flags,
                    requiredColumns
            );
        } else {
            pub = internalTable.scan(
                    partWithTerm.partId(),
                    txAttributes.id(),
                    new PrimaryReplica(ctx.localNode(), partWithTerm.term()),
                    indexId,
                    lower,
                    upper,
                    flags,
                    requiredColumns
            );
        }

        return new TransformingPublisher<>(pub, item -> rowConverter.toRow(ctx, item, rowFactory, requiredColumns));
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> Publisher<RowT> indexLookup(
            ExecutionContext<RowT> ctx,
            PartitionWithTerm partWithTerm,
            RowFactory<RowT> rowFactory,
            int indexId,
            List<Integer> columns,
            RowT key,
            @Nullable BitSet requiredColumns
    ) {

        BinaryTupleSchema indexRowSchema = RowConverter.createIndexRowSchema(columns, tableDescriptor);
        TxAttributes txAttributes = ctx.txAttributes();
        Publisher<BinaryRow> pub;

        BinaryTuple keyTuple = toBinaryTuple(ctx, indexRowSchema, key, rowFactory);

        if (txAttributes.readOnly()) {
            pub = internalTable.lookup(
                    partWithTerm.partId(),
                    txAttributes.time(),
                    ctx.localNode(),
                    indexId,
                    keyTuple,
                    requiredColumns
            );
        } else {
            pub = internalTable.lookup(
                    partWithTerm.partId(),
                    txAttributes.id(),
                    new PrimaryReplica(ctx.localNode(), partWithTerm.term()),
                    indexId,
                    keyTuple,
                    requiredColumns
            );
        }

        return new TransformingPublisher<>(pub, item -> rowConverter.toRow(ctx, item, rowFactory, requiredColumns));
    }

    private <RowT> @Nullable BinaryTuplePrefix toBinaryTuplePrefix(ExecutionContext<RowT> ctx,
            BinaryTupleSchema indexRowSchema,
            @Nullable RowT condition, RowFactory<RowT> factory) {

        if (condition == null) {
            return null;
        }

        return RowConverter.toBinaryTuplePrefix(ctx, indexRowSchema, factory, condition);
    }

    private <RowT> @Nullable BinaryTuple toBinaryTuple(ExecutionContext<RowT> ctx, BinaryTupleSchema indexRowSchema,
            @Nullable RowT condition, RowFactory<RowT> factory) {
        if (condition == null) {
            return null;
        }

        return RowConverter.toBinaryTuple(ctx, indexRowSchema, factory, condition);
    }

}
