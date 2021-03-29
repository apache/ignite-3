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

package org.apache.ignite.table.impl;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.table.InvokeProcessor;
import org.jetbrains.annotations.NotNull;

/**
 * Dummy table storage implementation.
 */
public class DummyInternalTableImpl implements InternalTable {
    /** In-memory dummy store. */
    private final Map<KeyWrapper, BinaryRow> store = new ConcurrentHashMap<>();

    /**
     * Wrapper provides corrowt byte[] comparison.
     */
    private static class KeyWrapper {
        /** Data. */
        private final byte[] data;

        /** Hash. */
        private final int hash;

        /**
         * Constructor.
         *
         * @param data Wrapped data.
         */
        KeyWrapper(byte[] data, int hash) {
            assert data != null;

            this.data = data;
            this.hash = hash;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            KeyWrapper wrapper = (KeyWrapper)o;
            return Arrays.equals(data, wrapper.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hash;
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> get(@NotNull BinaryRow row) {
        assert row != null;

        final KeyWrapper key = extractAndWrapKey(row);

        return CompletableFuture.completedFuture(store.get(key));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsert(@NotNull BinaryRow row) {
        assert row != null;

        final KeyWrapper key = extractAndWrapKey(row);

        store.put(key, row);

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndUpsert(@NotNull BinaryRow row) {
        assert row != null;

        final KeyWrapper key = extractAndWrapKey(row);

        final BinaryRow old = store.put(key, row);

        return CompletableFuture.completedFuture(old);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> delete(BinaryRow row) {
        assert row != null;

        final KeyWrapper key = extractAndWrapKey(row);

        return CompletableFuture.completedFuture(store.remove(key) != null);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows) {
        assert keyRows != null && !keyRows.isEmpty();

        final List<BinaryRow> res = keyRows.stream()
            .map(this::extractAndWrapKey)
            .map(store::get)
            .collect(Collectors.toList());

        return CompletableFuture.completedFuture(res);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows) {
        assert rows != null && !rows.isEmpty();

        rows.stream()
            .map(k -> store.put(extractAndWrapKey(k), k));

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> insert(BinaryRow row) {
        assert row != null;

        return CompletableFuture.completedFuture(store.putIfAbsent(extractAndWrapKey(row), row) == null);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows) {
        assert rows != null && !rows.isEmpty();

        final List<BinaryRow> res = rows.stream()
            .map(k -> store.putIfAbsent(extractAndWrapKey(k), k) == null ? null : k)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        return CompletableFuture.completedFuture(res);
    }

    @Override public @NotNull CompletableFuture<Boolean> replace(BinaryRow row) {
        assert row != null;

        return CompletableFuture.completedFuture(store.replace(extractAndWrapKey(row), row) != null);
    }

    @Override public @NotNull CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow) {
        return null;
    }

    @Override public @NotNull CompletableFuture<BinaryRow> getAndReplace(BinaryRow row) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteExact(BinaryRow row) {
        assert row != null;
        assert row.hasValue();
        
        final KeyWrapper key = extractAndWrapKey(row);
        final BinaryRow old = store.get(key);
        
        if (old == null || !old.hasValue())
            return CompletableFuture.completedFuture(false);

        assert row.schemaVersion() == old.schemaVersion() : "Table doesn't support row version upgrade.";

        final ByteBuffer val = row.valueSlice();
        final ByteBuffer oldVal = old.valueSlice();


        if (val.remaining() != oldVal.remaining())
            return CompletableFuture.completedFuture(false);

        int i = 0;

        while (i < val.limit() && val.get(i) == val.get(i))
            i++;

        return CompletableFuture.completedFuture(i == val.limit());
    }

    @Override public @NotNull CompletableFuture<BinaryRow> getAndDelete(BinaryRow row) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows) {
        return null;
    }

    @Override public @NotNull <T extends Serializable, R> CompletableFuture<T> invoke(BinaryRow keyRow,
        InvokeProcessor<R, R, T> proc) {
        return null;
    }

    @Override public @NotNull <T extends Serializable, R> CompletableFuture<Map<BinaryRow, T>> invokeAll(
        Collection<BinaryRow> keyRows, InvokeProcessor<R, R, T> proc) {
        return null;
    }

    /**
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private DummyInternalTableImpl.KeyWrapper extractAndWrapKey(@NotNull BinaryRow row) {
        final byte[] bytes = new byte[row.keySlice().capacity()];
        row.keySlice().get(bytes);

        return new KeyWrapper(bytes, row.hash());
    }
}
