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

package org.apache.ignite.internal.sql.engine.table;

import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.index.Index;
import org.apache.ignite.internal.index.IndexDescriptor;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/** Test Hash index implementation. */
public class TestHashIndex implements Index<IndexDescriptor> {
    private final UUID id = UUID.randomUUID();

    private UUID tableId = UUID.randomUUID();

    private final IndexDescriptor descriptor;

    /** Create index. */
    public static TestHashIndex create(List<String> indexedColumns, String name, UUID tableId) {
        var descriptor = new IndexDescriptor(name, indexedColumns);

        TestHashIndex idx = new TestHashIndex(descriptor);

        idx.tableId = tableId;

        return idx;
    }

    /** Create index. */
    public static TestHashIndex create(List<String> indexedColumns, String name) {
        var descriptor = new IndexDescriptor(name, indexedColumns);

        return new TestHashIndex(descriptor);
    }

    TestHashIndex(IndexDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return descriptor.name();
    }

    /** {@inheritDoc} */
    @Override
    public UUID tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override
    public IndexDescriptor descriptor() {
        return descriptor;
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<BinaryRow> lookup(int partId, UUID txId, PrimaryReplica recipient, BinaryTuple key,
            @Nullable BitSet columns) {
        throw new AssertionError("Should not be called");
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<BinaryRow> lookup(int partId, HybridTimestamp timestamp, ClusterNode recipient, BinaryTuple key, BitSet columns) {
        throw new AssertionError("Should not be called");
    }
}
