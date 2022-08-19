/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.tx.Timestamp;
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
        final @Nullable Timestamp begin;
        final @Nullable UUID txId;
        final @Nullable VersionChain next;

        VersionChain(@Nullable BinaryRow row, @Nullable Timestamp begin, @Nullable UUID txId, @Nullable VersionChain next) {
            this.row = row;
            this.begin = begin;
            this.txId = txId;
            this.next = next;
        }

        static VersionChain createUncommitted(@Nullable BinaryRow row, @Nullable UUID txId, @Nullable VersionChain next) {
            return new VersionChain(row, null, txId, next);
        }

        static VersionChain createCommitted(@Nullable Timestamp timestamp, VersionChain uncommittedVersionChain) {
            return new VersionChain(uncommittedVersionChain.row, timestamp, null, uncommittedVersionChain.next);
        }

        boolean notContainsWriteIntent() {
            return begin != null && txId == null;
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
    @Override
    public RowId insert(BinaryRow row, UUID txId) throws StorageException {
        RowId rowId = new RowId(partitionId);

        addWrite(rowId, row, txId);

        return rowId;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId) throws TxIdMismatchException {
        BinaryRow[] res = {null};

        map.compute(rowId, (ignored, versionChain) -> {
            if (versionChain != null && versionChain.begin == null) {
                if (!txId.equals(versionChain.txId)) {
                    throw new TxIdMismatchException(txId, versionChain.txId);
                }

                res[0] = versionChain.row;

                return VersionChain.createUncommitted(row, txId, versionChain.next);
            }

            return VersionChain.createUncommitted(row, txId, versionChain);
        });

        return res[0];
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) {
        BinaryRow[] res = {null};

        map.computeIfPresent(rowId, (ignored, versionChain) -> {
            if (versionChain.notContainsWriteIntent()) {
                return versionChain;
            }

            assert versionChain.begin == null;

            res[0] = versionChain.row;

            return versionChain.next;
        });

        return res[0];
    }

    /** {@inheritDoc} */
    @Override
    public void commitWrite(RowId rowId, Timestamp timestamp) {
        map.compute(rowId, (ignored, versionChain) -> {
            assert versionChain != null;

            if (versionChain.notContainsWriteIntent()) {
                return versionChain;
            }

            return VersionChain.createCommitted(timestamp, versionChain);
        });
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow read(RowId rowId, UUID txId) throws TxIdMismatchException, StorageException {
        VersionChain versionChain = map.get(rowId);

        return read(versionChain, null, txId, null);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow read(RowId rowId, @Nullable Timestamp timestamp) {
        VersionChain versionChain = map.get(rowId);

        return read(versionChain, timestamp, null, null);
    }

    @Nullable
    private static BinaryRow read(
            VersionChain versionChain,
            @Nullable Timestamp timestamp,
            @Nullable UUID txId,
            @Nullable Predicate<BinaryRow> filter
    ) {
        assert timestamp == null ^ txId == null;

        if (versionChain == null) {
            return null;
        }

        if (timestamp == null) {
            BinaryRow binaryRow = versionChain.row;

            if (filter != null && !filter.test(binaryRow)) {
                return null;
            }

            if (versionChain.txId != null && !versionChain.txId.equals(txId)) {
                throw new TxIdMismatchException(txId, versionChain.txId);
            }

            return binaryRow;
        }

        VersionChain cur = versionChain;

        if (cur.begin == null) {
            cur = cur.next;
        }

        while (cur != null) {
            if (timestamp.compareTo(cur.begin) >= 0) {
                BinaryRow binaryRow = cur.row;

                if (filter != null && !filter.test(binaryRow)) {
                    return null;
                }

                return binaryRow;
            }

            cur = cur.next;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> filter, UUID txId) {
        Iterator<BinaryRow> iterator = map.values().stream()
                .map(versionChain -> read(versionChain, null, txId, filter))
                .filter(Objects::nonNull)
                .iterator();

        return Cursor.fromIterator(iterator);
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> filter, Timestamp timestamp) {
        Iterator<BinaryRow> iterator = map.values().stream()
                .map(versionChain -> read(versionChain, timestamp, null, filter))
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
