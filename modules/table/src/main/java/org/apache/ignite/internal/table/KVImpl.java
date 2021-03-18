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

package org.apache.ignite.internal.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.schema.marshaller.Marshaller;
import org.apache.ignite.internal.storage.TableStorage;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.ColSpan;
import org.apache.ignite.table.ColSpanBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * Key-value view implementation for binary objects.
 *
 * @implNote Key-value objects are wrappers over corresponding column spans and implement the binary object concept.
 */
public class KVImpl implements KeyValueBinaryView {
    /** Underlying storage. */
    private final TableStorage tbl;

    /**
     * Constructor.
     *
     * @param tbl Table storage.
     */
    public KVImpl(TableStorage tbl) {
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public ColSpan get(ColSpan keySpan) {
        Objects.requireNonNull(keySpan);

        return marshaller().marshallKVPair(keySpan, null);
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<ColSpan> getAsync(ColSpan key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Map<ColSpan, ColSpan> getAll(Collection<ColSpan> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Map<ColSpan, ColSpan>> getAllAsync(Collection<ColSpan> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(ColSpan key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void put(ColSpan keySpan, ColSpan valSpan) {
        Objects.requireNonNull(keySpan);

        final TableRow row = marshaller().marshallKVPair(keySpan, valSpan);

        tbl.put(row);
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> putAsync(ColSpan key, ColSpan val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<ColSpan, ColSpan> pairs) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> putAllAsync(Map<ColSpan, ColSpan> pairs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ColSpan getAndPut(ColSpan key, ColSpan val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<ColSpan> getAndPutAsync(ColSpan key, ColSpan val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(ColSpan key, ColSpan val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> putIfAbsentAsync(ColSpan key, ColSpan val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(ColSpan key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> removeAsync(ColSpan key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(ColSpan key, ColSpan val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> removeAsync(ColSpan key, ColSpan val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<ColSpan> removeAll(Collection<ColSpan> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<ColSpan> removeAllAsync(Collection<ColSpan> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ColSpan getAndRemove(ColSpan key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<ColSpan> getAndRemoveAsync(ColSpan key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(ColSpan key, ColSpan val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(ColSpan key, ColSpan val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(ColSpan key, ColSpan oldVal, ColSpan newVal) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(ColSpan key, ColSpan oldVal,
        ColSpan newVal) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ColSpan getAndReplace(ColSpan key, ColSpan val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<ColSpan> getAndReplaceAsync(ColSpan key, ColSpan val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> R invoke(
        ColSpan key,
        InvokeProcessor<ColSpan, ColSpan, R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> IgniteFuture<R> invokeAsync(
        ColSpan key,
        InvokeProcessor<ColSpan, ColSpan, R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> Map<ColSpan, R> invokeAll(
        Collection<ColSpan> keys,
        InvokeProcessor<ColSpan, ColSpan, R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> IgniteFuture<Map<ColSpan, R>> invokeAllAsync(
        Collection<ColSpan> keys,
        InvokeProcessor<ColSpan, ColSpan, R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ColSpanBuilder binaryBuilder() {
        return null;
    }

    /**
     * @return Marshaller.
     */
    private Marshaller marshaller() {
        return null;
    }
}
