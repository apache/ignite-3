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

import static java.util.Comparator.comparing;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.AbortResult;
import org.apache.ignite.internal.storage.AddWriteCommittedResult;
import org.apache.ignite.internal.storage.AddWriteResult;
import org.apache.ignite.internal.storage.CommitResult;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.RowMeta;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageDestroyedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.storage.util.LocalLocker;
import org.apache.ignite.internal.storage.util.LockByRowId;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of MV partition storage.
 */
public class TestMvPartitionStorage implements MvPartitionStorage {
    /** Preserved {@link LocalLocker} instance to allow nested calls of {@link #runConsistently(WriteClosure)}. */
    private static final ThreadLocal<LocalLocker> THREAD_LOCAL_LOCKER = new ThreadLocal<>();

    private static final AtomicLongFieldUpdater<TestMvPartitionStorage> ESTIMATED_SIZE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(TestMvPartitionStorage.class, "estimatedSize");

    private final ConcurrentNavigableMap<RowId, VersionChain> map = new ConcurrentSkipListMap<>();

    private final NavigableSet<VersionChain> gcQueue = new ConcurrentSkipListSet<>(
            comparing((VersionChain chain) -> chain.ts)
                    .thenComparing(chain -> chain.rowId)
    );

    private volatile long lastAppliedIndex;

    private volatile long lastAppliedTerm;

    @Nullable
    private volatile LeaseInfo leaseInfo;

    private volatile long estimatedSize;

    private volatile byte @Nullable [] groupConfig;

    final int partitionId;

    private volatile boolean closed;

    private volatile boolean destroyed;

    private volatile boolean rebalance;

    private final LockByRowId lockByRowId;

    private final BooleanSupplier shouldReleaseSupplier;

    /** Amount of cursors that opened and still do not close. */
    private final AtomicInteger pendingCursors = new AtomicInteger();

    public TestMvPartitionStorage(int partitionId) {
        this(partitionId, new LockByRowId());
    }

    public TestMvPartitionStorage(int partitionId, LockByRowId lockByRowId) {
        this(partitionId, lockByRowId, () -> false);
    }

    /**
     * This constructor allows for creating a test partition storage with custom lock release behavior,
     * which is useful for testing scenarios where lock contention needs to be simulated (e.g., during
     * rebalancing or when other operations need to acquire locks held by long-running operations like GC).
     *
     * @param partitionId Partition ID.
     * @param lockByRowId Shared lock manager for row-level locking.
     * @param shouldReleaseSupplier Supplier that determines when locks should be released. When this supplier
     *        returns {@code true}, operations holding locks (like GC vacuum) should exit early to allow
     *        other operations to proceed. See
     *        {@link Locker#shouldRelease()} for more details.
     */
    public TestMvPartitionStorage(
            int partitionId,
            LockByRowId lockByRowId,
            BooleanSupplier shouldReleaseSupplier
    ) {
        this.partitionId = partitionId;
        this.lockByRowId = lockByRowId;
        this.shouldReleaseSupplier = shouldReleaseSupplier;
    }

    private static class VersionChain implements GcEntry {
        private final RowId rowId;
        private final @Nullable BinaryRow row;
        private final @Nullable HybridTimestamp ts;
        private final @Nullable UUID txId;
        private final @Nullable Integer commitZoneId;
        private final int commitPartitionId;
        volatile @Nullable VersionChain next;

        VersionChain(
                RowId rowId,
                @Nullable BinaryRow row,
                @Nullable HybridTimestamp ts,
                @Nullable UUID txId,
                @Nullable Integer commitZoneId,
                int commitPartitionId,
                @Nullable VersionChain next
        ) {
            this.rowId = rowId;
            this.row = row;
            this.ts = ts;
            this.txId = txId;
            this.commitZoneId = commitZoneId;
            this.commitPartitionId = commitPartitionId;
            this.next = next;
        }

        static VersionChain forWriteIntent(RowId rowId, @Nullable BinaryRow row, @Nullable UUID txId, @Nullable Integer commitZoneId,
                int commitPartitionId, @Nullable VersionChain next) {
            return new VersionChain(rowId, row, null, txId, commitZoneId, commitPartitionId, next);
        }

        static VersionChain forCommitted(RowId rowId, HybridTimestamp timestamp, VersionChain uncommittedVersionChain) {
            return new VersionChain(rowId, uncommittedVersionChain.row, timestamp, null, null,
                    ReadResult.UNDEFINED_COMMIT_PARTITION_ID, uncommittedVersionChain.next);
        }

        boolean isWriteIntent() {
            return ts == null && txId != null;
        }

        @Override
        public RowId getRowId() {
            return rowId;
        }

        @Override
        public HybridTimestamp getTimestamp() {
            assert ts != null : "Method should only be invoked for instances with non-null timestamps.";

            return ts;
        }

        @Override
        public String toString() {
            return S.toString(VersionChain.class, this);
        }
    }

    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        checkStorageClosed();

        LocalLocker locker = THREAD_LOCAL_LOCKER.get();

        if (locker != null) {
            return closure.execute(locker);
        } else {
            locker = new TestStorageLocker();

            THREAD_LOCAL_LOCKER.set(locker);

            try {
                return closure.execute(locker);
            } finally {
                THREAD_LOCAL_LOCKER.set(null);

                locker.unlockAll();
            }
        }
    }

    @Override
    public CompletableFuture<Void> flush(boolean trigger) {
        checkStorageClosed();

        return nullCompletedFuture();
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
        checkStorageClosedOrInProcessOfRebalance();

        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }

    @Override
    public byte @Nullable [] committedGroupConfiguration() {
        checkStorageClosed();

        byte[] currentConfig = groupConfig;
        return currentConfig == null ? null : Arrays.copyOf(currentConfig, currentConfig.length);
    }

    @Override
    public void committedGroupConfiguration(byte[] config) {
        checkStorageClosedOrInProcessOfRebalance();

        this.groupConfig = Arrays.copyOf(config, config.length);
    }

    @Override
    public synchronized AddWriteResult addWrite(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitZoneId,
            int commitPartitionId
    ) throws StorageException {
        assert rowId.partitionId() == partitionId : "rowId=" + rowId + ", rowIsTombstone=" + (row == null) + ", txId=" + txId
                + ", commitZoneId=" + commitZoneId + ", commitPartitionId=" + commitPartitionId;

        checkStorageClosed();

        AddWriteResult[] addWriteResult = {null};

        map.compute(rowId, (ignored, versionChain) -> {
            if (versionChain != null && versionChain.ts == null) {
                if (!txId.equals(versionChain.txId)) {
                    addWriteResult[0] = AddWriteResult.txMismatch(versionChain.txId, latestCommitTimestamp(versionChain));

                    return versionChain;
                }

                addWriteResult[0] = AddWriteResult.success(versionChain.row);

                return VersionChain.forWriteIntent(rowId, row, txId, commitZoneId, commitPartitionId, versionChain.next);
            }

            addWriteResult[0] = AddWriteResult.success(null);

            return VersionChain.forWriteIntent(rowId, row, txId, commitZoneId, commitPartitionId, versionChain);
        });

        AddWriteResult res = addWriteResult[0];

        assert res != null : "rowId=" + rowId + ", rowIsTombstone=" + (row == null) + ", txId=" + txId
                + ", commitZoneId=" + commitZoneId + ", commitPartitionId=" + commitPartitionId;

        return res;
    }

    @Override
    public synchronized AbortResult abortWrite(RowId rowId, UUID txId) {
        assert rowId.partitionId() == partitionId : "rowId=" + rowId + ", txId=" + txId;

        checkStorageClosedOrInProcessOfRebalance();

        AbortResult[] abortResult = {null};

        map.compute(rowId, (ignored, versionChain) -> {
            if (versionChain == null || !versionChain.isWriteIntent()) {
                abortResult[0] = AbortResult.noWriteIntent();

                return versionChain;
            } else if (!txId.equals(versionChain.txId)) {
                abortResult[0] = AbortResult.txMismatch(versionChain.txId);

                return versionChain;
            }

            assert versionChain.ts == null : "rowId=" + rowId + ", txId=" + txId + ", ts=" + versionChain.ts;

            abortResult[0] = AbortResult.success(versionChain.row);

            return versionChain.next;
        });

        AbortResult res = abortResult[0];

        assert res != null : "rowId=" + rowId + ", txId=" + txId;

        return res;
    }

    @Override
    public synchronized CommitResult commitWrite(RowId rowId, HybridTimestamp timestamp, UUID txId) {
        assert rowId.partitionId() == partitionId : "rowId=" + rowId + ", timestamp=" + timestamp + ", txId=" + txId;

        checkStorageClosed();

        CommitResult[] commitResult = {null};

        map.compute(rowId, (ignored, versionChain) -> {
            if (versionChain == null || !versionChain.isWriteIntent()) {
                commitResult[0] = CommitResult.noWriteIntent();

                return versionChain;
            } else if (!txId.equals(versionChain.txId)) {
                commitResult[0] = CommitResult.txMismatch(versionChain.txId);

                return versionChain;
            }

            commitResult[0] = CommitResult.success();

            return resolveCommittedVersionChain(VersionChain.forCommitted(rowId, timestamp, versionChain));
        });

        CommitResult res = commitResult[0];

        assert res != null : "rowId=" + rowId + ", timestamp=" + timestamp + ", txId=" + txId;

        return res;
    }

    @Override
    public synchronized AddWriteCommittedResult addWriteCommitted(
            RowId rowId,
            @Nullable BinaryRow row,
            HybridTimestamp commitTimestamp
    ) throws StorageException {
        assert rowId.partitionId() == partitionId : "rowId=" + rowId + ", rowIsTombstone=" + (row == null)
                + ", commitTimestamp=" + commitTimestamp;

        checkStorageClosed();

        AddWriteCommittedResult[] addWriteCommittedResult = {null};

        map.compute(rowId, (ignored, versionChain) -> {
            if (versionChain != null && versionChain.isWriteIntent()) {
                addWriteCommittedResult[0] = AddWriteCommittedResult.writeIntentExists(
                        versionChain.txId,
                        latestCommitTimestamp(versionChain)
                );

                return versionChain;
            }

            addWriteCommittedResult[0] = AddWriteCommittedResult.success();

            return resolveCommittedVersionChain(new VersionChain(
                    rowId,
                    row,
                    commitTimestamp,
                    null,
                    null,
                    ReadResult.UNDEFINED_COMMIT_PARTITION_ID,
                    versionChain
            ));
        });

        AddWriteCommittedResult res = addWriteCommittedResult[0];

        assert res != null : "rowId=" + rowId + ", rowIsTombstone=" + (row == null) + ", commitTimestamp=" + commitTimestamp;

        return res;
    }

    private @Nullable VersionChain resolveCommittedVersionChain(VersionChain committedVersionChain) {
        VersionChain nextChain = committedVersionChain.next;

        boolean isNewValueTombstone = committedVersionChain.row == null;

        if (nextChain != null) {
            boolean isOldValueTombstone = nextChain.row == null;

            if (isOldValueTombstone) {
                if (isNewValueTombstone) {
                    // Avoid creating tombstones for tombstones.
                    return nextChain;
                }

                ESTIMATED_SIZE_UPDATER.incrementAndGet(this);
            } else if (isNewValueTombstone) {
                ESTIMATED_SIZE_UPDATER.decrementAndGet(this);
            }

            // Calling it from the compute is fine. Concurrent writes of the same row are impossible, and if we call the compute closure
            // several times, the same tuple will be inserted into the GC queue (timestamp and rowId don't change in this case).
            gcQueue.add(committedVersionChain);
        } else {
            if (isNewValueTombstone) {
                // If there is only one version, and it is a tombstone, then remove the chain.
                return null;
            }

            ESTIMATED_SIZE_UPDATER.incrementAndGet(this);
        }

        return committedVersionChain;
    }

    @Override
    public ReadResult read(RowId rowId, HybridTimestamp timestamp) {
        checkStorageClosedOrInProcessOfRebalance();

        if (rowId.partitionId() != partitionId) {
            throw new IllegalArgumentException(
                    String.format("RowId partition [%d] is not equal to storage partition [%d].", rowId.partitionId(), partitionId));
        }

        VersionChain versionChain = map.get(rowId);

        if (versionChain == null) {
            return ReadResult.empty(rowId);
        }

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

                return ReadResult.createFromWriteIntent(cur.rowId, binaryRow, cur.txId, cur.commitZoneId, cur.commitPartitionId, null);
            }

            // Move to first commit.
            cur = cur.next;
        }

        return walkVersionChain(versionChain, timestamp, cur);
    }

    private static ReadResult versionChainToReadResult(VersionChain versionChain, boolean fillLastCommittedTs) {
        if (versionChain.isWriteIntent()) {
            VersionChain next = versionChain.next;

            return ReadResult.createFromWriteIntent(
                    versionChain.rowId,
                    versionChain.row,
                    versionChain.txId,
                    versionChain.commitZoneId,
                    versionChain.commitPartitionId, fillLastCommittedTs && next != null ? next.ts : null
            );
        }

        return ReadResult.createFromCommitted(versionChain.rowId, versionChain.row, versionChain.ts);
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

            return ReadResult.createFromWriteIntent(
                    chainHead.rowId,
                    binaryRow,
                    chainHead.txId,
                    chainHead.commitZoneId,
                    chainHead.commitPartitionId,
                    firstCommit.ts);
        }

        VersionChain cur = firstCommit;

        while (cur != null) {
            assert cur.ts != null;

            if (timestamp.compareTo(cur.ts) >= 0) {
                // This commit has timestamp matching the query ts, meaning that commit is the one we are looking for.
                BinaryRow binaryRow = cur.row;

                return ReadResult.createFromCommitted(cur.rowId, binaryRow, cur.ts);
            }

            cur = cur.next;
        }

        return ReadResult.empty(chainHead.rowId);
    }

    @Override
    public Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException {
        checkStorageClosedOrInProcessOfRebalance();

        return new ScanVersionsCursor(rowId);
    }

    /**
     * Gets amount of pending cursors.
     *
     * @return Amount of pending cursors.
     */
    public int pendingCursors() {
        return pendingCursors.get();
    }

    @Override
    public PartitionTimestampCursor scan(HybridTimestamp timestamp) {
        checkStorageClosedOrInProcessOfRebalance();

        Iterator<VersionChain> iterator = map.values().iterator();

        pendingCursors.incrementAndGet();

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

                return read(currentChain, timestamp, null).binaryRow();
            }

            @Override
            public void close() {
                pendingCursors.decrementAndGet();
            }

            @Override
            public boolean hasNext() {
                checkStorageClosedOrInProcessOfRebalance();

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

                assert res != null;

                currentReadResult = null;

                return res;
            }
        };
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) throws StorageException {
        checkStorageClosedOrInProcessOfRebalance();

        return map.ceilingKey(lowerBound);
    }

    @Override
    public @Nullable RowId highestRowId() throws StorageException {
        checkStorageClosedOrInProcessOfRebalance();

        return map.floorKey(RowId.highestRowId(partitionId));
    }

    @Override
    public List<RowMeta> rowsStartingWith(RowId lowerBoundInclusive, RowId upperBoundInclusive, int limit) throws StorageException {
        checkStorageClosedOrInProcessOfRebalance();

        List<RowMeta> result = new ArrayList<>();
        RowId currentLowerBound = lowerBoundInclusive;
        for (int i = 0; i < limit; i++) {
            RowMeta row = closestRow(currentLowerBound);

            if (row == null || row.rowId().compareTo(upperBoundInclusive) > 0) {
                break;
            }

            result.add(row);
            currentLowerBound = row.rowId().increment();

            if (currentLowerBound == null) {
                break;
            }
        }

        return result;
    }

    private @Nullable RowMeta closestRow(RowId lowerBound) throws StorageException {
        Entry<RowId, VersionChain> entry = map.ceilingEntry(lowerBound);
        if (entry == null) {
            return null;
        }

        VersionChain versionChain = entry.getValue();

        return new RowMeta(versionChain.rowId, versionChain.txId, versionChain.commitZoneId, versionChain.commitPartitionId);
    }

    @Override
    public synchronized List<GcEntry> peek(HybridTimestamp lowWatermark, int count) {
        if (count <= 0) {
            return List.of();
        }

        var res = new ArrayList<GcEntry>(count);

        Iterator<VersionChain> it = gcQueue.iterator();

        for (int i = 0; i < count && it.hasNext(); i++) {
            VersionChain next = it.next();

            if (next.ts.compareTo(lowWatermark) > 0) {
                break;
            }

            res.add(next);
        }

        return res;
    }

    @Override
    public synchronized @Nullable BinaryRow vacuum(GcEntry entry) {
        assert THREAD_LOCAL_LOCKER.get() != null;
        assert THREAD_LOCAL_LOCKER.get().isLocked(entry.getRowId());

        checkStorageClosedOrInProcessOfRebalance();

        VersionChain dequeuedVersionChain;

        try {
            dequeuedVersionChain = gcQueue.first();
        } catch (NoSuchElementException e) {
            return null;
        }

        if (dequeuedVersionChain != entry) {
            return null;
        }

        RowId rowId = dequeuedVersionChain.rowId;

        VersionChain versionChainToRemove = dequeuedVersionChain.next;
        assert versionChainToRemove != null;
        assert versionChainToRemove.next == null;

        dequeuedVersionChain.next = null;
        gcQueue.remove(dequeuedVersionChain);

        // Tombstones must be deleted.
        if (dequeuedVersionChain.row == null) {
            map.compute(rowId, (ignored, head) -> {
                if (head == dequeuedVersionChain) {
                    return null;
                }

                for (VersionChain cur = head; cur != null; cur = cur.next) {
                    if (cur.next == dequeuedVersionChain) {
                        cur.next = null;

                        gcQueue.remove(cur);
                    }
                }

                return head;
            });
        }

        return versionChainToRemove.row;
    }

    @Override
    public synchronized void updateLease(LeaseInfo leaseInfo) {
        checkStorageClosed();

        LeaseInfo thisLeaseInfo = this.leaseInfo;

        if (thisLeaseInfo != null && leaseInfo.leaseStartTime() <= thisLeaseInfo.leaseStartTime()) {
            return;
        }

        this.leaseInfo = leaseInfo;
    }

    @Override
    public @Nullable LeaseInfo leaseInfo() {
        checkStorageClosed();

        return leaseInfo;
    }

    @Override
    public long estimatedSize() {
        checkStorageClosed();

        return estimatedSize;
    }

    @Override
    public void close() {
        if (rebalance) {
            throw new StorageRebalanceException();
        }

        closed = true;

        clear0();
    }

    /**
     * Destroys this storage.
     */
    public void destroy() {
        destroyed = true;

        clear0();
    }

    /** Removes all entries from this storage. */
    public synchronized void clear() {
        checkStorageClosedOrInProcessOfRebalance();

        clear0();
    }

    private synchronized void clear0() {
        map.clear();

        gcQueue.clear();

        lastAppliedIndex = 0;
        lastAppliedTerm = 0;
        estimatedSize = 0;
        groupConfig = null;
        leaseInfo = null;
    }

    private void checkStorageClosed() {
        if (closed) {
            throw new StorageClosedException();
        }
        if (destroyed) {
            throw new StorageDestroyedException();
        }
    }

    private void checkStorageClosedForRebalance() {
        if (closed || destroyed) {
            throw new StorageRebalanceException();
        }
    }

    private void checkStorageInProcessOfRebalance() {
        if (rebalance) {
            throw new StorageRebalanceException();
        }
    }

    private void checkStorageClosedOrInProcessOfRebalance() {
        checkStorageClosed();
        checkStorageInProcessOfRebalance();
    }

    void startRebalance() {
        checkStorageClosedForRebalance();

        rebalance = true;

        clear0();

        lastAppliedIndex = REBALANCE_IN_PROGRESS;
        lastAppliedTerm = REBALANCE_IN_PROGRESS;
    }

    void abortRebalance() {
        checkStorageClosedForRebalance();

        if (!rebalance) {
            return;
        }

        rebalance = false;

        clear0();
    }

    void finishRebalance(MvPartitionMeta partitionMeta) {
        checkStorageClosedForRebalance();

        assert rebalance;

        rebalance = false;

        this.lastAppliedIndex = partitionMeta.lastAppliedIndex();
        this.lastAppliedTerm = partitionMeta.lastAppliedTerm();
        this.groupConfig = Arrays.copyOf(partitionMeta.groupConfig(), partitionMeta.groupConfig().length);
        this.leaseInfo = partitionMeta.leaseInfo();
    }

    private class ScanVersionsCursor implements Cursor<ReadResult> {
        private final RowId rowId;

        @Nullable
        private Boolean hasNext;

        @Nullable
        private VersionChain versionChain;

        private ScanVersionsCursor(RowId rowId) {
            this.rowId = rowId;
        }

        @Override
        public void close() {
            // No-op.
        }

        @Override
        public boolean hasNext() {
            advanceIfNeeded();

            return hasNext;
        }

        @Override
        public ReadResult next() {
            advanceIfNeeded();

            if (!hasNext) {
                throw new NoSuchElementException();
            }

            hasNext = null;

            return versionChainToReadResult(versionChain, false);
        }

        private void advanceIfNeeded() {
            checkStorageClosedOrInProcessOfRebalance();

            if (hasNext != null) {
                return;
            }

            versionChain = versionChain == null ? map.get(rowId) : versionChain.next;

            hasNext = versionChain != null;
        }
    }

    /**
     * Checks if current thread holds a lock on passed row ID.
     */
    static boolean rowIsLocked(RowId rowId) {
        LocalLocker locker = THREAD_LOCAL_LOCKER.get();

        return locker != null && locker.isLocked(rowId);
    }

    private static @Nullable HybridTimestamp latestCommitTimestamp(VersionChain chain) {
        VersionChain next = chain.next;

        return next == null ? null : next.ts;
    }

    private class TestStorageLocker extends LocalLocker {
        private TestStorageLocker() {
            super(lockByRowId);
        }

        @Override
        public boolean shouldRelease() {
            return shouldReleaseSupplier.getAsBoolean();
        }
    }
}
