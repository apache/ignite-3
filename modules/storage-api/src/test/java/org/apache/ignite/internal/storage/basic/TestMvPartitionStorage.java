/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.basic;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.IgniteRowId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.storage.UuidIgniteRowId;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of MV partition storage.
 */
public class TestMvPartitionStorage implements MvPartitionStorage {
    private final ConcurrentMap<IgniteRowId, VersionChain> map = new ConcurrentHashMap<>();

    private final List<TestSortedIndexMvStorage> indexes;

    public TestMvPartitionStorage(List<TestSortedIndexMvStorage> indexes) {
        this.indexes = indexes;
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

        public static VersionChain createUncommitted(BinaryRow row, UUID txId, VersionChain next) {
            return new VersionChain(row, null, txId, next);
        }

        public static VersionChain createCommitted(Timestamp timestamp, VersionChain uncommittedVersionChain) {
            return new VersionChain(uncommittedVersionChain.row, timestamp, null, uncommittedVersionChain.next);
        }
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRowId insert(BinaryRow row, UUID txId) throws StorageException {
        IgniteRowId rowId = UuidIgniteRowId.randomRowId(0);

        addWrite(rowId, row, txId);

        return rowId;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow addWrite(IgniteRowId rowId, @Nullable BinaryRow row, UUID txId) throws TxIdMismatchException {
        BinaryRow[] res = {null};

        map.compute(rowId, (ignored, versionChain) -> {
            if (versionChain != null && versionChain.begin == null) {
                if (!txId.equals(versionChain.txId)) {
                    throw new TxIdMismatchException();
                }

                cleanupIndexesForAbortedRow(versionChain, rowId);

                res[0] = versionChain.row;

                return VersionChain.createUncommitted(row, txId, versionChain.next);
            }

            return VersionChain.createUncommitted(row, txId, versionChain);
        });

        if (row != null) {
            for (TestSortedIndexMvStorage index : indexes) {
                index.append(row, rowId);
            }
        }

        return res[0];
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow abortWrite(IgniteRowId rowId) {
        BinaryRow[] res = {null};

        map.computeIfPresent(rowId, (ignored, versionChain) -> {
            assert versionChain != null;
            assert versionChain.begin == null && versionChain.txId != null;

            cleanupIndexesForAbortedRow(versionChain, rowId);

            res[0] = versionChain.row;

            return versionChain.next;
        });

        return res[0];
    }

    private void abortWrite(IgniteRowId rowId, VersionChain head, BinaryRow aborted, TestSortedIndexMvStorage index) {
        for (VersionChain cur = head; cur != null; cur = cur.next) {
            if (index.matches(aborted, cur.row)) {
                return;
            }
        }

        index.remove(aborted, rowId);
    }

    private void cleanupIndexesForAbortedRow(VersionChain versionChain, IgniteRowId rowId) {
        if (versionChain.row != null) {
            for (TestSortedIndexMvStorage index : indexes) {
                abortWrite(rowId, versionChain.next, versionChain.row, index);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void commitWrite(IgniteRowId rowId, Timestamp timestamp) {
        map.compute(rowId, (ignored, versionChain) -> {
            assert versionChain != null;
            assert versionChain.begin == null && versionChain.txId != null;

            return VersionChain.createCommitted(timestamp, versionChain);
        });
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public BinaryRow read(IgniteRowId rowId, @Nullable Timestamp timestamp) {
        VersionChain versionChain = map.get(rowId);

        return read(versionChain, timestamp);
    }

    @Nullable
    private BinaryRow read(VersionChain versionChain, @Nullable Timestamp timestamp) {
        if (versionChain == null) {
            return null;
        }

        if (timestamp == null) {
            return versionChain.row;
        }

        VersionChain cur = versionChain;

        if (cur.begin == null) {
            cur = cur.next;
        }

        while (cur != null) {
            if (timestamp.compareTo(cur.begin) >= 0) {
                return cur.row;
            }

            cur = cur.next;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, @Nullable Timestamp timestamp) {
        Iterator<BinaryRow> iterator = map.values().stream()
                .map(versionChain -> read(versionChain, timestamp))
                .filter(Objects::nonNull)
                .filter(keyFilter)
                .iterator();

        return Cursor.fromIterator(iterator);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        // No-op.
    }
}
