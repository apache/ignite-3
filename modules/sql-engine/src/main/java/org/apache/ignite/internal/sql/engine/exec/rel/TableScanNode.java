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
import java.util.Iterator;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.metadata.PartitionWithTerm;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.internal.util.TransformingIterator;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.jetbrains.annotations.Nullable;

/**
 * Execution node for table scan. Provide result of table scan by given table and partitions.
 */
public class TableScanNode<RowT> extends StorageScanNode<RowT> {

    /** Table that provides access to underlying data. */
    private final InternalTable physTable;

    /** List of pairs containing the partition number to scan with the corresponding primary replica term. */
    private final Collection<PartitionWithTerm> partsWithTerms;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowFactory Row factory.
     * @param schemaTable The table this node should scan.
     * @param partsWithTerms List of pairs containing the partition number to scan with the corresponding primary replica term.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     * @param requiredColumns Optional set of column of interest.
     */
    public TableScanNode(
            ExecutionContext<RowT> ctx,
            RowHandler.RowFactory<RowT> rowFactory,
            IgniteTable schemaTable,
            Collection<PartitionWithTerm> partsWithTerms,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable BitSet requiredColumns
    ) {
        super(ctx, rowFactory, schemaTable, filters, rowTransformer, requiredColumns);

        assert partsWithTerms != null && !partsWithTerms.isEmpty();

        this.physTable = schemaTable.table();
        this.partsWithTerms = partsWithTerms;
    }

    /** {@inheritDoc} */
    @Override
    protected Publisher<RowT> scan() {
        TxAttributes txAttributes = context().txAttributes();
        Iterator<Publisher<? extends RowT>> it = new TransformingIterator<>(
                partsWithTerms.iterator(), partWithTerm -> {
            Publisher<BinaryRow> pub;

            if (txAttributes.readOnly()) {
                pub = physTable.scan(partWithTerm.partId(), txAttributes.time(), context().localNode());
            } else {
                PrimaryReplica recipient = new PrimaryReplica(context().localNode(), partWithTerm.term());

                pub = physTable.scan(partWithTerm.partId(), txAttributes.id(), recipient, null, null, null, 0, null);
            }

            return convertPublisher(pub);
        });

        return SubscriptionUtils.concat(it);
    }
}
