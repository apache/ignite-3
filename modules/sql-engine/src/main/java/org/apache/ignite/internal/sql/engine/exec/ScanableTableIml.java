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
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.rel.StorageScanNode;
import org.apache.ignite.internal.sql.engine.metadata.PartitionWithTerm;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ScanableTable} that uses {@link InternalTable}.
 */
public class ScanableTableIml implements ScanableTable {

    private final InternalTable internalTable;

    private final TableRowConverter rowConverter;

    /** Constructor. */
    public ScanableTableIml(InternalTable internalTable, TableRowConverter rowConverter) {
        this.internalTable = internalTable;
        this.rowConverter = rowConverter;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> Publisher<RowT> scan(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
            RowFactory<RowT> rowFactory, @Nullable BitSet requiredColumns) {

        Publisher<BinaryRow> pub;
        TxAttributes txAttributes1 = ctx.txAttributes();

        if (txAttributes1.readOnly()) {
            pub = internalTable.scan(partWithTerm.partId(), txAttributes1.time(), ctx.localNode());
        } else {
            PrimaryReplica recipient = new PrimaryReplica(ctx.localNode(), partWithTerm.term());

            pub = internalTable.scan(partWithTerm.partId(), txAttributes1.id(), recipient, null, null, null, 0, null);
        }

        return StorageScanNode.convertPublisher(pub, (item) -> {
            return rowConverter.toRow(ctx, item, rowFactory, requiredColumns);
        });
    }
}
