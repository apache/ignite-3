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

package org.apache.ignite.internal.storage.chm;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of MV partition storage.
 */
public class TestConcurrentHashMapMvPartitionStorage implements MvPartitionStorage {
    private final ConcurrentMap<RowId, VersionChain> map = new ConcurrentHashMap<>();

    private long lastAppliedIndex = 0;

    private final int partitionId;

    public TestConcurrentHashMapMvPartitionStorage(int partitionId) {
        this.partitionId = partitionId;
    }

    private static class VersionChain {
        final @Nullable BinaryRow row;
        final @Nullable HybridTimestamp ts;
        final @Nullable UUID txId;
        final @Nullable UUID commitTableId;
        final int commitPartitionId;
        final @Nullable VersionChain next;

        VersionChain(@Nullable BinaryRow row, @Nullable HybridTimestamp ts, @Nullable UUID txId, @Nullable UUID commitTableId,
                int commitPartitionId, @Nullable VersionChain next) {
            this.row = row;
            this.ts = ts;
            this.txId = txId;
            this.commitTableId = commitTableId;
            this.commitPartitionId = commitPartitionId;
            this.next = next;
        }

        static VersionChain forWriteIntent(@Nullable BinaryRow row, @Nullable UUID txId, @Nullable UUID commitTableId,
                int commitPartitionId, @Nullable VersionChain next) {
            return new VersionChain(row, null, txId, commitTableId, commitPartitionId, next);
        }

        static VersionChain forCommitted(@Nullable HybridTimestamp timestamp, VersionChain uncommittedVersionChain) {
            return new VersionChain(uncommittedVersionChain.row, timestamp, null, null,
                    ReadResult.UNDEFINED_COMMIT_PARTITION_ID, uncommittedVersionChain.next);
        }

        boolean isWriteIntent() {
            return ts == null && txId != null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        return closure.execute();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> flush() {
        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override
    public void lastAppliedIndex(long lastAppliedIndex) throws StorageException {
        assert lastAppliedIndex > this.lastAppliedIndex : "current=" + this.lastAppliedIndex + ", new=" + lastAppliedIndex;

        this.lastAppliedIndex = lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override
    public long persistedIndex() {
        return lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override
    public RowId insert(BinaryRow row, UUID txId) throws StorageException {
        RowId rowId = new RowId(partitionId);

        addWrite(rowId, row, txId, UUID.randomUUID(), 0);

        return rowId;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, UUID commitTableId, int commitPartitionId)
            throws TxIdMismatchException {
        BinaryRow[] res = {null};

        map.compute(rowId, (ignored, versionChain) -> {
            if (versionChain != null && versionChain.ts == null) {
                if (!txId.equals(versionChain.txId)) {
                    throw new TxIdMismatchException(txId, versionChain.txId);
                }

                res[0] = versionChain.row;

                return VersionChain.forWriteIntent(row, txId, commitTableId, commitPartitionId, versionChain.next);
            }

            return VersionChain.forWriteIntent(row, txId, commitTableId, commitPartitionId, versionChain);
        });

        return res[0];
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) {
        BinaryRow[] res = {null};

        map.computeIfPresent(rowId, (ignored, versionChain) -> {
            if (!versionChain.isWriteIntent()) {
                return versionChain;
            }

            assert versionChain.ts == null;

            res[0] = versionChain.row;

            return versionChain.next;
        });

        return res[0];
    }

    /** {@inheritDoc} */
    @Override
    public void commitWrite(RowId rowId, HybridTimestamp timestamp) {
        map.compute(rowId, (ignored, versionChain) -> {
            assert versionChain != null;

            if (!versionChain.isWriteIntent()) {
                return versionChain;
            }

            return VersionChain.forCommitted(timestamp, versionChain);
        });
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow read(RowId rowId, UUID txId) throws TxIdMismatchException, StorageException {
        if (rowId.partitionId() != partitionId) {
            throw new IllegalArgumentException(
                    String.format("RowId partition [%d] is not equal to storage partition [%d].", rowId.partitionId(), partitionId));
        }

        VersionChain versionChain = map.get(rowId);

        return read(versionChain, null, txId, null).binaryRow();
    }

    /** {@inheritDoc} */
    @Override
    public ReadResult read(RowId rowId, @Nullable HybridTimestamp timestamp) {
        if (rowId.partitionId() != partitionId) {
            throw new IllegalArgumentException(
                    String.format("RowId partition [%d] is not equal to storage partition [%d].", rowId.partitionId(), partitionId));
        }

        VersionChain versionChain = map.get(rowId);

        return read(versionChain, timestamp, null, null);
    }

    private static ReadResult read(
            VersionChain versionChain,
            @Nullable HybridTimestamp timestamp,
            @Nullable UUID txId,
            @Nullable Predicate<BinaryRow> filter
    ) {
        assert timestamp == null ^ txId == null;

        if (versionChain == null) {
            return ReadResult.empty();
        }

        if (timestamp == null) {
            BinaryRow binaryRow = versionChain.row;

            if (filter != null && !filter.test(binaryRow)) {
                return ReadResult.empty();
            }

            if (versionChain.txId != null && !versionChain.txId.equals(txId)) {
                throw new TxIdMismatchException(txId, versionChain.txId);
            }

            boolean isWriteIntent = versionChain.ts == null;

            if (isWriteIntent) {
                return ReadResult.createFromWriteIntent(
                        binaryRow,
                        versionChain.txId,
                        versionChain.commitTableId,
                        versionChain.next != null ? versionChain.next.ts : null,
                        versionChain.commitPartitionId
                );
            }

            return ReadResult.createFromCommitted(binaryRow);
        }

        VersionChain cur = versionChain;

        if (cur.ts == null) {
            if (cur.next == null) {
                // We only have a write-intent.
                BinaryRow binaryRow = cur.row;

                if (filter != null && !filter.test(binaryRow)) {
                    return ReadResult.empty();
                }

                return ReadResult.createFromWriteIntent(binaryRow, cur.txId, cur.commitTableId, null,
                        cur.commitPartitionId);
            }

            cur = cur.next;
        }

        return walkVersionChain(versionChain, timestamp, filter, cur);
    }

    private static ReadResult walkVersionChain(VersionChain chainHead, HybridTimestamp timestamp, @Nullable Predicate<BinaryRow> filter,
            VersionChain firstCommit) {
        boolean hasWriteIntent = chainHead.ts == null;

        if (hasWriteIntent && timestamp.compareTo(firstCommit.ts) > 0) {
            // It's the latest commit in chain, query ts is greater than commit ts and there is a write-intent.
            // So we just return write-intent.
            BinaryRow binaryRow = chainHead.row;

            if (filter != null && !filter.test(binaryRow)) {
                return ReadResult.empty();
            }

            HybridTimestamp latestCommitTs = chainHead.next != null ? firstCommit.ts : null;

            return ReadResult.createFromWriteIntent(binaryRow, chainHead.txId, chainHead.commitTableId, latestCommitTs,
                    chainHead.commitPartitionId);
        }

        VersionChain cur = firstCommit;

        while (cur != null) {
            int compareResult = timestamp.compareTo(cur.ts);

            if (compareResult >= 0) {
                // This commit has timestamp matching the query ts, meaning that commit is the one we are looking for.
                BinaryRow binaryRow = cur.row;

                if (filter != null && !filter.test(binaryRow)) {
                    return ReadResult.empty();
                }

                return ReadResult.createFromCommitted(binaryRow);
            }

            cur = cur.next;
        }

        return ReadResult.empty();
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> filter, UUID txId) {
        Iterator<BinaryRow> iterator = map.values().stream()
                .map(versionChain -> read(versionChain, null, txId, filter))
                .map(ReadResult::binaryRow)
                .filter(Objects::nonNull)
                .iterator();

        return Cursor.fromIterator(iterator);
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> filter, HybridTimestamp timestamp) {
        Iterator<BinaryRow> iterator = map.values().stream()
                .map(versionChain -> read(versionChain, timestamp, null, filter))
                .map(ReadResult::binaryRow)
                .filter(Objects::nonNull)
                .iterator();

        return Cursor.fromIterator(iterator);
    }

    /** {@inheritDoc} */
    @Override
    public long rowsCount() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override
    public void forEach(BiConsumer<RowId, BinaryRow> consumer) {
        for (Entry<RowId, VersionChain> entry : map.entrySet()) {
            RowId rowId = entry.getKey();

            VersionChain versionChain = entry.getValue();

            for (VersionChain cur = versionChain; cur != null; cur = cur.next) {
                if (cur.row == null) {
                    continue;
                }

                consumer.accept(rowId, cur.row);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        // No-op.
    }
}
