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
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Execution node for table scan. Provide result of table scan by given table and partitions.
 */
public class TableScanNode<RowT> extends StorageScanNode<RowT> {

    /** Table that provides access to underlying data. */
    private final InternalTable physTable;

    private final int[] parts;

    /** Raft terms of the partition group leaders. */
    private final long[] terms;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowFactory Row factory.
     * @param schemaTable The table this node should scan.
     * @param parts Partition numbers to scan.
     * @param terms Raft terms of the partition group leaders.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     * @param requiredColumns Optional set of column of interest.
     */
    public TableScanNode(
            ExecutionContext<RowT> ctx,
            RowHandler.RowFactory<RowT> rowFactory,
            InternalIgniteTable schemaTable,
            int[] parts,
            long @Nullable[] terms,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable BitSet requiredColumns
    ) {
        super(ctx, rowFactory, schemaTable, filters, rowTransformer, requiredColumns);

        assert !nullOrEmpty(parts);
        assert ctx.transactionTime() != null || (terms != null && parts.length == terms.length);

        this.physTable = schemaTable.table();
        this.parts = parts;
        this.terms = terms;
    }

    /** {@inheritDoc} */
    @Override
    protected Publisher<RowT> scan() {
        boolean readOnlyTx = context().transactionTime() != null;
        List<Publisher<? extends RowT>> publishers = new ArrayList<>(parts.length);

        for (int i = 0; i < parts.length; i++) {
            int partId = parts[i];

            Publisher<BinaryRow> pub;

            if (readOnlyTx) {
                pub = physTable.scan(partId, context().transactionTime(), context().localNode());
            } else if (context().transaction() != null) {
                // TODO IGNITE-17952 This block should be removed.
                // Workaround to make RW scan work from tx coordinator.
                pub = physTable.scan(partId, context().transaction());
            } else {
                pub = physTable.scan(partId, context().transactionId(), context().localNode(), terms[i], null, null, null, 0, null);
            }

            publishers.add(convertPublisher(pub));
        }

        return SubscriptionUtils.concat(publishers.iterator());
    }
}
