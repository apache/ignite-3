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

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.sql.engine.util.TransferredTxAttributesHolder;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.internal.util.TransformingIterator;
import org.jetbrains.annotations.Nullable;

/**
 * Execution node for table scan. Provide result of table scan by given table and partitions.
 */
public class TableScanNode<RowT> extends StorageScanNode<RowT> {

    /** Table that provides access to underlying data. */
    private final InternalTable physTable;

    private final ColocationGroup mapping;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowFactory Row factory.
     * @param schemaTable The table this node should scan.
     * @param mapping Target mapping.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     * @param requiredColumns Optional set of column of interest.
     */
    public TableScanNode(
            ExecutionContext<RowT> ctx,
            RowHandler.RowFactory<RowT> rowFactory,
            InternalIgniteTable schemaTable,
            ColocationGroup mapping,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable BitSet requiredColumns
    ) {
        super(ctx, rowFactory, schemaTable, filters, rowTransformer, requiredColumns);

        assert !nullOrEmpty(mapping.partitions(ctx.localNode().name()));

        this.physTable = schemaTable.table();
        this.mapping = mapping;
    }

    /** {@inheritDoc} */
    @Override
    protected Publisher<RowT> scan() {
        InternalTransaction tx = context().transaction();
        int[] parts = mapping.partitions(context().localNode().name());

        Iterator<Publisher<? extends RowT>> it = new TransformingIterator<>(
                Arrays.stream(parts).iterator(), part -> {
            Publisher<BinaryRow> pub;

            if (tx.isReadOnly()) {
                pub = physTable.scan(part, tx.readTimestamp(), context().localNode());
            } else if (!(tx instanceof TransferredTxAttributesHolder)) {
                // TODO IGNITE-17952 This block should be removed.
                // Workaround to make RW scan work from tx coordinator.
                pub = physTable.scan(part, tx);
            } else {
                pub = physTable.scan(part, tx.id(), context().localNode(), mapping.partitionLeaderTerm(part), null, null, null, 0, null);
            }

            return convertPublisher(pub);
        });

        return SubscriptionUtils.concat(it);
    }
}
