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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.table.InvokeProcessor;
import org.jetbrains.annotations.NotNull;

/**
 * Dummy table storage implementation.
 */
public class DummyTableStorageImpl implements InternalTable {
    /** In-memory dummy store. */
    private final Map<KeyWrapper, BinaryRow> store = new ConcurrentHashMap<>();

    /**
     * Wrapper provides correct byte[] comparison.
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

    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRecs) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Void> upsertAll(Collection<BinaryRow> recs) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Boolean> insert(BinaryRow rec) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> recs) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Boolean> replace(BinaryRow rec) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Boolean> replace(BinaryRow oldRec, BinaryRow newRec) {
        return null;
    }

    @Override public @NotNull CompletableFuture<BinaryRow> getAndReplace(BinaryRow rec) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Boolean> deleteExact(BinaryRow oldRec) {
        return null;
    }

    @Override public @NotNull CompletableFuture<BinaryRow> getAndDelete(BinaryRow rec) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> recs) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> recs) {
        return null;
    }

    @Override public @NotNull <T extends Serializable, R> CompletableFuture<T> invoke(BinaryRow keyRec,
        InvokeProcessor<R, R, T> proc) {
        return null;
    }

    @Override public @NotNull <T extends Serializable, R> CompletableFuture<Map<BinaryRow, T>> invokeAll(
        Collection<BinaryRow> keyRecs, InvokeProcessor<R, R, T> proc) {
        return null;
    }

    /**
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private DummyTableStorageImpl.KeyWrapper extractAndWrapKey(@NotNull BinaryRow row) {
        final byte[] bytes = new byte[row.keySlice().capacity()];
        row.keySlice().get(bytes);

        return new KeyWrapper(bytes, row.hash());
    }
}
