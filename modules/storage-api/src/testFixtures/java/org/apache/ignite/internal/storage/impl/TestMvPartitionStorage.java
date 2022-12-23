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

package org.apache.ignite.internal.storage.impl;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageFullRebalanceException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of MV partition storage.
 */
public class TestMvPartitionStorage implements MvPartitionStorage {
    private final ConcurrentNavigableMap<RowId, VersionChain> map = new ConcurrentSkipListMap<>();

    private volatile long lastAppliedIndex;

    private volatile long lastAppliedTerm;

    @Nullable
    private volatile RaftGroupConfiguration groupConfig;

    private final int partitionId;

    private volatile boolean closed;

    private volatile boolean fullRebalance;

    public TestMvPartitionStorage(int partitionId) {
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

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        checkStorageClosed();

        return closure.execute();
    }

    @Override
    public CompletableFuture<Void> flush() {
        checkStorageClosed();

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public long lastAppliedIndex() {
        checkStorageClosed();

        return lastAppliedIndex;
    }

    @Override
    public long lastAppliedTerm() {
        checkStorageClosed();

        return lastAppliedTerm;
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        checkStorageClosedOrInProcessFullRebalance();

        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }

    @Override
    public long persistedIndex() {
        checkStorageClosed();

        return lastAppliedIndex;
    }

    @Override
    @Nullable
    public RaftGroupConfiguration committedGroupConfiguration() {
        checkStorageClosed();

        return groupConfig;
    }

    @Override
    public void committedGroupConfiguration(RaftGroupConfiguration config) {
        checkStorageClosedOrInProcessFullRebalance();

        this.groupConfig = config;
    }

    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, UUID commitTableId, int commitPartitionId)
            throws TxIdMismatchException {
        checkStorageClosed();

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

    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) {
        checkStorageClosed();

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

    @Override
    public void commitWrite(RowId rowId, HybridTimestamp timestamp) {
        checkStorageClosed();

        map.compute(rowId, (ignored, versionChain) -> {
            assert versionChain != null;

            if (!versionChain.isWriteIntent()) {
                return versionChain;
            }

            return VersionChain.forCommitted(timestamp, versionChain);
        });
    }

    @Override
    public void addWriteCommitted(RowId rowId, BinaryRow row, HybridTimestamp commitTimestamp) throws StorageException {
        checkStorageClosed();

        map.compute(rowId, (ignored, versionChain) -> {
            if (versionChain != null && versionChain.isWriteIntent()) {
                throw new StorageException("Write intent exists for " + rowId);
            }

            return new VersionChain(row, commitTimestamp, null, null, ReadResult.UNDEFINED_COMMIT_PARTITION_ID, versionChain);
        });
    }

    @Override
    public ReadResult read(RowId rowId, @Nullable HybridTimestamp timestamp) {
        checkStorageClosedOrInProcessFullRebalance();

        if (rowId.partitionId() != partitionId) {
            throw new IllegalArgumentException(
                    String.format("RowId partition [%d] is not equal to storage partition [%d].", rowId.partitionId(), partitionId));
        }

        VersionChain versionChain = map.get(rowId);

        return read(versionChain, timestamp, null);
    }

    /**
     * Reads the value from the version chain using either transaction id or timestamp.
     *
     * @param versionChain Version chain.
     * @param timestamp Timestamp or {@code null} if transaction id is defined.
     * @param txId Transaction id or {@code null} if timestamp is defined.
     * @return Read result.
     */
    private static ReadResult read(
            VersionChain versionChain,
            @Nullable HybridTimestamp timestamp,
            @Nullable UUID txId
    ) {
        assert timestamp == null ^ txId == null;

        if (versionChain == null) {
            return ReadResult.EMPTY;
        }

        if (timestamp == null) {
            // Search by transaction id.

            if (versionChain.txId != null && !versionChain.txId.equals(txId)) {
                throw new TxIdMismatchException(txId, versionChain.txId);
            }

            return versionChainToReadResult(versionChain, true);
        }

        VersionChain cur = versionChain;

        if (cur.isWriteIntent()) {
            // We have a write-intent.
            if (cur.next == null) {
                // We *only* have a write-intent, return it.
                BinaryRow binaryRow = cur.row;

                return ReadResult.createFromWriteIntent(binaryRow, cur.txId, cur.commitTableId, cur.commitPartitionId, null);
            }

            // Move to first commit.
            cur = cur.next;
        }

        return walkVersionChain(versionChain, timestamp, cur);
    }

    private static ReadResult versionChainToReadResult(VersionChain versionChain, boolean fillLastCommittedTs) {
        if (versionChain.isWriteIntent()) {
            return ReadResult.createFromWriteIntent(
                    versionChain.row,
                    versionChain.txId,
                    versionChain.commitTableId,
                    versionChain.commitPartitionId, fillLastCommittedTs && versionChain.next != null ? versionChain.next.ts : null
            );
        }

        return ReadResult.createFromCommitted(versionChain.row, versionChain.ts);
    }

    /**
     * Walks version chain to find a row by timestamp. See {@link MvPartitionStorage#read(RowId, HybridTimestamp)} for details.
     *
     * @param chainHead Version chain head.
     * @param timestamp Timestamp.
     * @param firstCommit First commit chain element.
     * @return Read result.
     */
    private static ReadResult walkVersionChain(VersionChain chainHead, HybridTimestamp timestamp, VersionChain firstCommit) {
        boolean hasWriteIntent = chainHead.ts == null;

        if (hasWriteIntent && timestamp.compareTo(firstCommit.ts) > 0) {
            // It's the latest commit in chain, query ts is greater than commit ts and there is a write-intent.
            // So we just return write-intent.
            BinaryRow binaryRow = chainHead.row;

            return ReadResult.createFromWriteIntent(binaryRow, chainHead.txId, chainHead.commitTableId, chainHead.commitPartitionId,
                    firstCommit.ts);
        }

        VersionChain cur = firstCommit;

        while (cur != null) {
            assert cur.ts != null;

            if (timestamp.compareTo(cur.ts) >= 0) {
                // This commit has timestamp matching the query ts, meaning that commit is the one we are looking for.
                BinaryRow binaryRow = cur.row;

                return ReadResult.createFromCommitted(binaryRow, cur.ts);
            }

            cur = cur.next;
        }

        return ReadResult.EMPTY;
    }

    @Override
    public Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException {
        checkStorageClosedOrInProcessFullRebalance();

        return new Cursor<>() {
            @Nullable
            private Boolean hasNext;

            @Nullable
            private VersionChain versionChain;

            @Override
            public void close() {
                // No-op.
            }

            @Override
            public boolean hasNext() {
                checkStorageClosedOrInProcessFullRebalance();

                advanceIfNeeded();

                return hasNext;
            }

            @Override
            public ReadResult next() {
                checkStorageClosedOrInProcessFullRebalance();

                advanceIfNeeded();

                if (!hasNext) {
                    throw new NoSuchElementException();
                }

                hasNext = null;

                return versionChainToReadResult(versionChain, false);
            }

            private void advanceIfNeeded() {
                if (hasNext != null) {
                    return;
                }

                versionChain = versionChain == null ? map.get(rowId) : versionChain.next;

                hasNext = versionChain != null;
            }
        };
    }

    @Override
    public PartitionTimestampCursor scan(HybridTimestamp timestamp) {
        checkStorageClosedOrInProcessFullRebalance();

        Iterator<VersionChain> iterator = map.values().iterator();

        return new PartitionTimestampCursor() {
            @Nullable
            private VersionChain currentChain;

            @Nullable
            private ReadResult currentReadResult;

            @Override
            public @Nullable BinaryRow committed(HybridTimestamp timestamp) {
                if (currentChain == null) {
                    throw new IllegalStateException();
                }

                // We don't check if row conforms the key filter here, because we've already checked it.
                ReadResult read = read(currentChain, timestamp, null);

                if (read.transactionId() == null) {
                    return read.binaryRow();
                }

                return null;
            }

            @Override
            public void close() {
                // No-op.
            }

            @Override
            public boolean hasNext() {
                checkStorageClosedOrInProcessFullRebalance();

                if (currentReadResult != null) {
                    return true;
                }

                currentChain = null;

                while (iterator.hasNext()) {
                    VersionChain chain = iterator.next();
                    ReadResult readResult = read(chain, timestamp, null);

                    if (!readResult.isEmpty() || readResult.isWriteIntent()) {
                        currentChain = chain;
                        currentReadResult = readResult;

                        return true;
                    }
                }

                return false;
            }

            @Override
            public ReadResult next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                ReadResult res = currentReadResult;

                currentReadResult = null;

                return res;
            }
        };
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) throws StorageException {
        checkStorageClosedOrInProcessFullRebalance();

        return map.ceilingKey(lowerBound);
    }

    @Override
    public long rowsCount() {
        checkStorageClosedOrInProcessFullRebalance();

        return map.size();
    }

    @Override
    public void close() {
        closed = true;

        clear();
    }

    public void destroy() {
        close();
    }

    /** Removes all entries from this storage. */
    public void clear() {
        map.clear();
    }

    private void checkStorageClosed() {
        if (closed) {
            throw new StorageClosedException("Storage is already closed");
        }
    }

    private void checkStorageClosedOrInProcessFullRebalance() {
        checkStorageClosed();

        if (fullRebalance) {
            throw new StorageFullRebalanceException("Storage in the process of a full rebalancing");
        }
    }

    void startFullRebalance() {
        checkStorageClosed();

        fullRebalance = true;

        clear();

        lastAppliedIndex = FULL_REBALANCE_IN_PROGRESS;
        lastAppliedTerm = FULL_REBALANCE_IN_PROGRESS;
    }

    void abortFullRebalance() {
        checkStorageClosed();

        fullRebalance = false;

        clear();

        lastAppliedIndex = 0;
        lastAppliedTerm = 0;
    }

    void finishFullRebalance(long lastAppliedIndex, long lastAppliedTerm) {
        checkStorageClosed();

        assert fullRebalance;

        fullRebalance = false;

        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }
}
