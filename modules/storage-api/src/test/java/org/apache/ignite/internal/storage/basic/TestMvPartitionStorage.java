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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of MV partition storage.
 */
public class TestMvPartitionStorage implements MvPartitionStorage {
    private final ConcurrentMap<ByteBuffer, VersionChain> map = new ConcurrentHashMap<>();

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
    public void addWrite(BinaryRow row, UUID txId) throws TxIdMismatchException {
        map.compute(row.keySlice(), (keyBuf, versionChain) -> {
            if (versionChain != null && versionChain.begin == null && !txId.equals(versionChain.txId)) {
                throw new TxIdMismatchException();
            }

            return VersionChain.createUncommitted(row, txId, versionChain);
        });

        if (row.hasValue()) {
            for (TestSortedIndexMvStorage index : indexes) {
                index.append(row);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void abortWrite(BinaryRow key) {
        map.computeIfPresent(key.keySlice(), (ignored, versionChain) -> {
            assert versionChain != null;
            assert versionChain.begin == null && versionChain.txId != null;

            BinaryRow aborted = versionChain.row;

            if (aborted.hasValue()) {
                for (TestSortedIndexMvStorage index : indexes) {
                    abortWrite(versionChain.next, aborted, index);
                }
            }

            return versionChain.next;
        });
    }

    private void abortWrite(VersionChain head, BinaryRow aborted, TestSortedIndexMvStorage index) {
        for (VersionChain cur = head; cur != null; cur = cur.next) {
            if (index.matches(aborted, cur.row)) {
                return;
            }
        }

        index.remove(aborted);
    }

    /** {@inheritDoc} */
    @Override
    public void commitWrite(BinaryRow key, Timestamp timestamp) {
        map.compute(key.keySlice(), (keyBuf, versionChain) -> {
            assert versionChain != null;
            assert versionChain.begin == null && versionChain.txId != null;

            return VersionChain.createCommitted(timestamp, versionChain);
        });
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public BinaryRow read(BinaryRow key, @Nullable Timestamp timestamp) {
        VersionChain versionChain = map.get(key.keySlice());

        return read(versionChain, timestamp);
    }

    @Nullable
    private BinaryRow read(VersionChain versionChain, @Nullable Timestamp timestamp) {
        if (versionChain == null) {
            return null;
        }

        if (timestamp == null) {
            return versionChain.row.hasValue() ? versionChain.row : null;
        }

        VersionChain cur = versionChain;

        if (cur.begin == null) {
            cur = cur.next;
        }

        while (cur != null) {
            if (timestamp.compareTo(cur.begin) >= 0) {
                BinaryRow row = cur.row;

                return row.hasValue() ? row : null;
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
}
