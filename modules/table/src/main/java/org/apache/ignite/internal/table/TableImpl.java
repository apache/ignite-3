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
import org.apache.ignite.internal.schema.marshaller.Marshaller;
import org.apache.ignite.internal.storage.TableStorage;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.ColSpan;
import org.apache.ignite.table.ColSpanBuilder;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;
import org.jetbrains.annotations.NotNull;

/**
 * Table view implementation for binary objects.
 */
public class TableImpl implements Table {
    /** Table. */
    private final TableStorage tbl;

    /**
     * Constructor.
     *
     * @param tbl Table.
     */
    public TableImpl(TableStorage tbl) {
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        return new RecordViewImpl<>(tbl, recMapper);
    }

    /** {@inheritDoc} */
    @Override public <K, V> KeyValueView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        return new KVViewImpl<>(tbl, keyMapper, valMapper);
    }

    /** {@inheritDoc} */
    @Override public KeyValueBinaryView kvView() {
        return new KVImpl(tbl);
    }

    /** {@inheritDoc} */
    @Override public ColSpan get(ColSpan keyRec) {
        Marshaller marsh = marshaller();

        return tbl.get(marsh.marshallRecord(keyRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<ColSpan> getAsync(ColSpan keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<ColSpan> getAll(Collection<ColSpan> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<ColSpan>> getAllAsync(Collection<ColSpan> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void upsert(ColSpan rec) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> upsertAsync(ColSpan rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(Collection<ColSpan> recs) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> upsertAllAsync(Collection<ColSpan> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ColSpan getAndUpsert(ColSpan rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<ColSpan> getAndUpsertAsync(ColSpan rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean insert(ColSpan rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> insertAsync(ColSpan rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<ColSpan> insertAll(Collection<ColSpan> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<ColSpan>> insertAllAsync(Collection<ColSpan> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(ColSpan rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(ColSpan rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(ColSpan oldRec, ColSpan newRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(ColSpan oldRec, ColSpan newRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ColSpan getAndReplace(ColSpan rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<ColSpan> getAndReplaceAsync(ColSpan rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean delete(ColSpan keyRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> deleteAsync(ColSpan keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(ColSpan oldRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> deleteExactAsync(ColSpan oldRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ColSpan getAndDelete(ColSpan rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<ColSpan> getAndDeleteAsync(ColSpan rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<ColSpan> deleteAll(Collection<ColSpan> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<ColSpan>> deleteAllAsync(Collection<ColSpan> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<ColSpan> deleteAllExact(Collection<ColSpan> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Collection<ColSpan>> deleteAllExactAsync(
        Collection<ColSpan> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(
        ColSpan keyRec,
        InvokeProcessor<ColSpan, ColSpan, T> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> IgniteFuture<T> invokeAsync(
        ColSpan keyRec,
        InvokeProcessor<ColSpan, ColSpan, T> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<ColSpan, T> invokeAll(
        Collection<ColSpan> keyRecs,
        InvokeProcessor<ColSpan, ColSpan, T> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> IgniteFuture<Map<ColSpan, T>> invokeAllAsync(
        Collection<ColSpan> keyRecs,
        InvokeProcessor<ColSpan, ColSpan, T> proc
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
