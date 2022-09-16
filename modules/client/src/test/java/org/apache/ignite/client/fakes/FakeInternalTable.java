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

package org.apache.ignite.client.fakes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Publisher;
import javax.naming.OperationNotSupportedException;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.Nullable;

/**
 * Fake internal table.
 */
public class FakeInternalTable implements InternalTable {
    /** Table name. */
    private final String tableName;

    /** Table ID. */
    private final UUID tableId;

    /** Table data. */
    private final ConcurrentHashMap<ByteBuffer, BinaryRow> data = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param tableName Name.
     * @param tableId   Id.
     */
    public FakeInternalTable(String tableName, UUID tableId) {
        this.tableName = tableName;
        this.tableId = tableId;
    }

    /** {@inheritDoc} */
    @Override
    public MvTableStorage storage() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /** {@inheritDoc} */
    @Override
    public int partitions() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override
    public UUID tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return tableName;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, @Nullable InternalTransaction tx) {
        return CompletableFuture.completedFuture(data.get(keyRow.keySlice()));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRowEx> keyRows,
            @Nullable InternalTransaction tx) {
        var res = new ArrayList<BinaryRow>();

        for (var key : keyRows) {
            var val = get(key, null);

            if (val != null) {
                res.add(val.getNow(null));
            }
        }

        return CompletableFuture.completedFuture(res);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsert(BinaryRowEx row, @Nullable InternalTransaction tx) {
        data.put(row.keySlice(), row);

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        for (var row : rows) {
            upsert(row, tx);
        }

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndUpsert(BinaryRowEx row,
            @Nullable InternalTransaction tx) {
        var res = get(row, tx);

        upsert(row, tx);

        return CompletableFuture.completedFuture(res.getNow(null));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> insert(BinaryRowEx row, @Nullable InternalTransaction tx) {
        var old = get(row, tx).getNow(null);

        if (old == null) {
            upsert(row, tx);

            return CompletableFuture.completedFuture(true);
        }

        return CompletableFuture.completedFuture(false);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        var skipped = new ArrayList<BinaryRow>();

        for (var row : rows) {
            if (!insert(row, tx).getNow(null)) {
                skipped.add(row);
            }
        }

        return CompletableFuture.completedFuture(skipped);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx row, @Nullable InternalTransaction tx) {
        var old = get(row, tx).getNow(null);

        if (old == null) {
            return CompletableFuture.completedFuture(false);
        }

        return upsert(row, tx).thenApply(f -> true);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRowEx oldRow, BinaryRowEx newRow, @Nullable InternalTransaction tx) {
        var old = get(oldRow, tx).getNow(null);

        if (old == null || !old.valueSlice().equals(oldRow.valueSlice())) {
            return CompletableFuture.completedFuture(false);
        }

        return upsert(newRow, tx).thenApply(f -> true);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndReplace(BinaryRowEx row,
            @Nullable InternalTransaction tx) {
        var old = get(row, tx);

        return replace(row, tx).thenCompose(f -> old);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> delete(BinaryRowEx keyRow, @Nullable InternalTransaction tx) {
        var old = get(keyRow, tx).getNow(null);

        if (old != null) {
            data.remove(keyRow.keySlice());
        }

        return CompletableFuture.completedFuture(old != null);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteExact(BinaryRowEx oldRow, @Nullable InternalTransaction tx) {
        var old = get(oldRow, tx).getNow(null);

        if (old != null && old.valueSlice().equals(oldRow.valueSlice())) {
            data.remove(oldRow.keySlice());
            return CompletableFuture.completedFuture(true);
        }

        return CompletableFuture.completedFuture(false);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndDelete(BinaryRowEx row,
            @Nullable InternalTransaction tx) {
        var old = get(row, tx).getNow(null);

        if (old != null) {
            data.remove(row.keySlice());
        }

        return CompletableFuture.completedFuture(old);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        var skipped = new ArrayList<BinaryRow>();

        for (var row : rows) {
            if (!delete(row, tx).getNow(false)) {
                skipped.add(row);
            }
        }

        return CompletableFuture.completedFuture(skipped);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRowEx> rows, @Nullable InternalTransaction tx) {
        var skipped = new ArrayList<BinaryRow>();

        for (var row : rows) {
            if (!deleteExact(row, tx).getNow(false)) {
                skipped.add(row);
            }
        }

        return CompletableFuture.completedFuture(skipped);
    }

    /** {@inheritDoc} */
    @Override
    public Publisher<BinaryRow> scan(int p, @Nullable InternalTransaction tx) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /** {@inheritDoc} */
    @Override
    public List<String> assignments() {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /** {@inheritDoc} */
    @Override
    public ClusterNode leaderAssignment(int partition) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /** {@inheritDoc} */
    @Override
    public RaftGroupService partitionRaftGroupService(int partition) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public int partition(BinaryRowEx keyRow) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        // No-op.
    }
}
