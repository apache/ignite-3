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

package org.apache.ignite.internal.index;

import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * An object that represents a sorted index.
 */
public class SortedIndexImpl implements SortedIndex {
    private final UUID id;
    private final InternalTable table;
    private final SortedIndexDescriptor descriptor;

    /**
     * Constructs the sorted index.
     *
     * @param id An identifier of the index.
     * @param table A table this index relates to.
     * @param descriptor A descriptor of the index.
     */
    public SortedIndexImpl(UUID id, TableImpl table, SortedIndexDescriptor descriptor) {
        this.id = Objects.requireNonNull(id, "id");
        this.table = Objects.requireNonNull(table.internalTable(), "table");
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public UUID tableId() {
        return table.tableId();
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return descriptor.name();
    }

    /** {@inheritDoc} */
    @Override
    public SortedIndexDescriptor descriptor() {
        return descriptor;
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<BinaryRow> lookup(int partId, @Nullable InternalTransaction tx, BinaryTuple key, @Nullable BitSet columns) {
        return table.lookup(partId, tx, id, key, columns);
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<BinaryRow> lookup(
            int partId,
            HybridTimestamp timestamp,
            ClusterNode recipientNode,
            BinaryTuple key,
            @Nullable BitSet columns
    ) {
        return table.lookup(partId, timestamp, recipientNode, id, key, columns);
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            @Nullable InternalTransaction tx,
            @Nullable BinaryTuplePrefix leftBound,
            @Nullable BinaryTuplePrefix rightBound,
            int flags,
            @Nullable BitSet columnsToInclude
    ) {
        return table.scan(partId, tx, id, leftBound, rightBound, flags, columnsToInclude);
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<BinaryRow> scan(
            int partId,
            HybridTimestamp readTimestamp,
            ClusterNode recipientNode,
            @Nullable BinaryTuplePrefix leftBound,
            @Nullable BinaryTuplePrefix rightBound,
            int flags,
            @Nullable BitSet columnsToInclude
    ) {
        return table.scan(partId, readTimestamp, recipientNode, id, leftBound, rightBound, flags, columnsToInclude);
    }
}
