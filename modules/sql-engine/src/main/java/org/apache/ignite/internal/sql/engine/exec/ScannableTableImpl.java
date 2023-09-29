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

import static org.apache.ignite.internal.storage.index.SortedIndexStorage.GREATER_OR_EQUAL;
import static org.apache.ignite.internal.storage.index.SortedIndexStorage.LESS_OR_EQUAL;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.hlc.HybridTimestamp;
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

    private final TableRowConverterFactory converterFactory;

    private final TableDescriptor tableDescriptor;

    /** Constructor. */
    public ScannableTableImpl(InternalTable internalTable, TableRowConverterFactory converterFactory, TableDescriptor tableDescriptor) {
        this.internalTable = internalTable;
        this.converterFactory = converterFactory;
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

        TableRowConverter rowConverter = converterFactory.create(requiredColumns);

        return new TransformingPublisher<>(pub, item -> rowConverter.toRow(ctx, item, rowFactory));
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> Publisher<RowT> indexRangeScan(
            ExecutionContext<RowT> ctx,
            PartitionWithTerm partWithTerm,
            RowFactory<RowT> rowFactory,
            int indexId,
            List<String> columns,
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
            flags = LESS_OR_EQUAL | GREATER_OR_EQUAL;
            lower = null;
            upper = null;
        } else {
            lower = toBinaryTuplePrefix(ctx, indexRowSchema, cond.lower(), rowFactory);
            upper = toBinaryTuplePrefix(ctx, indexRowSchema, cond.upper(), rowFactory);

            flags |= (cond.lowerInclude()) ? GREATER_OR_EQUAL : 0;
            flags |= (cond.upperInclude()) ? LESS_OR_EQUAL : 0;
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

        TableRowConverter rowConverter = converterFactory.create(requiredColumns);

        return new TransformingPublisher<>(pub, item -> rowConverter.toRow(ctx, item, rowFactory));
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> Publisher<RowT> indexLookup(
            ExecutionContext<RowT> ctx,
            PartitionWithTerm partWithTerm,
            RowFactory<RowT> rowFactory,
            int indexId,
            List<String> columns, RowT key,
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

        TableRowConverter rowConverter = converterFactory.create(requiredColumns);

        return new TransformingPublisher<>(pub, item -> rowConverter.toRow(ctx, item, rowFactory));
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
