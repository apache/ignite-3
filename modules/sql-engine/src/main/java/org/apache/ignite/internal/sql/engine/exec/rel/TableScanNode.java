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
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.sql.engine.util.CompositePublisher;
import org.apache.ignite.internal.table.InternalTable;
import org.jetbrains.annotations.Nullable;

/**
 * Table scan node.
 */
public class TableScanNode<RowT> extends StorageScanNode<RowT> {

    /** Table that provides access to underlying data. */
    private final InternalTable physTable;

    private final int[] parts;

    boolean dataRequested;

    int curPartIdx = 0;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowFactory Row factory.
     * @param schemaTable The table this node should scan.
     * @param parts Partition numbers to scan.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     * @param requiredColumns Optional set of column of interest.
     */
    public TableScanNode(
            ExecutionContext<RowT> ctx,
            RowHandler.RowFactory<RowT> rowFactory,
            InternalIgniteTable schemaTable,
            int[] parts,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable BitSet requiredColumns
    ) {
        super(ctx, rowFactory, schemaTable, filters, rowTransformer, requiredColumns);

        assert !nullOrEmpty(parts);

        this.physTable = schemaTable.table();
        this.parts = parts;
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        super.rewindInternal();

        dataRequested = false;
        curPartIdx = 0;
    }

    /** {@inheritDoc} */
    @Override
    protected Publisher<RowT> scan() {
        Publisher<BinaryRow> scan = null;
        if (curPartIdx < parts.length) {

            if (context().transactionTime() != null) {
                scan = physTable.scan(parts[curPartIdx++], context().transactionTime(), context().localNode());
            } else {
                scan = physTable.scan(parts[curPartIdx++], context().transaction());
            }

            return convertPublisher(scan);
        }
        // after fix we can remove code above and uncomment this part.
        // IGNITE-
        //        if (!dataRequested) {
        //            dataRequested = true;
        //
        //            return scanPublisher(parts);
        //        }
        return null;
    }

    private Publisher<RowT> scanPublisher(int[] parts) {
        List<Flow.Publisher<BinaryRow>> partPublishers = new ArrayList<>(parts.length);

        for (int p : parts) {
            Publisher<BinaryRow> pub;
            if (context().transactionTime() != null) {
                pub = physTable.scan(p, context().transactionTime(), context().localNode());
            } else {
                pub = physTable.scan(p, context().transaction());
            }

            partPublishers.add(pub);
        }

        return convertPublisher(new CompositePublisher<>(partPublishers));
    }
}
