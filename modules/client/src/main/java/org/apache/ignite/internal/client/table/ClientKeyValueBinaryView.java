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

package org.apache.ignite.internal.client.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client key-value view implementation for binary user-object representation.
 */
public class ClientKeyValueBinaryView implements KeyValueBinaryView {
    /** Underlying table. */
    private final Table tbl;

    /**
     * Constructor.
     *
     * @param tbl Table.
     */
    public ClientKeyValueBinaryView(Table tbl) {
        assert tbl != null;

        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder tupleBuilder() {
        return tbl.tupleBuilder();
    }

    /** {@inheritDoc} */
    @Override public Tuple get(@NotNull Tuple key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAsync(@NotNull Tuple key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Map<Tuple, Tuple> getAll(@NotNull Collection<Tuple> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<Tuple, Tuple>> getAllAsync(@NotNull Collection<Tuple> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(@NotNull Tuple key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void put(@NotNull Tuple key, Tuple val) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAsync(@NotNull Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void putAll(@NotNull Map<Tuple, Tuple> pairs) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAllAsync(@NotNull Map<Tuple, Tuple> pairs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndPut(@NotNull Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndPutAsync(@NotNull Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(@NotNull Tuple key, @NotNull Tuple val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@NotNull Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@NotNull Tuple key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull Tuple key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@NotNull Tuple key, @NotNull Tuple val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull Tuple key, @NotNull Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> removeAll(@NotNull Collection<Tuple> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> removeAllAsync(@NotNull Collection<Tuple> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndRemove(@NotNull Tuple key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndRemoveAsync(@NotNull Tuple key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple key, Tuple val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple key, Tuple oldVal, Tuple newVal) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple key, Tuple oldVal, Tuple newVal) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(@NotNull Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@NotNull Tuple key, Tuple val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> R invoke(
            @NotNull Tuple key,
            InvokeProcessor<Tuple, Tuple, R> proc,
            Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(
            @NotNull Tuple key,
            InvokeProcessor<Tuple, Tuple, R> proc,
            Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> Map<Tuple, R> invokeAll(
            @NotNull Collection<Tuple> keys,
            InvokeProcessor<Tuple, Tuple, R> proc,
            Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> CompletableFuture<Map<Tuple, R>> invokeAllAsync(
            @NotNull Collection<Tuple> keys,
            InvokeProcessor<Tuple, Tuple, R> proc,
            Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Transaction transaction() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public KeyValueBinaryView withTransaction(Transaction tx) {
        return null;
    }
}
