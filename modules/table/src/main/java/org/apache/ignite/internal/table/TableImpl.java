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
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KV;
import org.apache.ignite.table.KVView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.binary.BinaryObject;
import org.apache.ignite.table.binary.BinaryObjectBuilder;
import org.apache.ignite.table.binary.Row;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;

/**
 * Table view implementation provides functionality to access binary BinaryObjects.
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
    @Override public <K, V> KVView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        return new KVViewImpl<>(tbl, keyMapper, valMapper);
    }

    /** {@inheritDoc} */
    @Override public KV kvView() {
        return new KVImpl(tbl);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject get(BinaryObject keyRec) {
        Row kRow = toKeyRow(keyRec);

        return tbl.get(kRow);
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryObject> getAll(Collection<BinaryObject> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean upsert(BinaryObject BinaryObject) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(Collection<BinaryObject> recs) {
    }

    /** {@inheritDoc} */
    @Override public boolean insert(BinaryObject BinaryObject) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryObject> insertAll(Collection<BinaryObject> recs) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public BinaryObject getAndUpsert(BinaryObject rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(BinaryObject rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(BinaryObject oldRec, BinaryObject newRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public BinaryObject getAndReplace(BinaryObject rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean delete(BinaryObject keyRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(BinaryObject oldRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public BinaryObject getAndDelete(BinaryObject rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public BinaryObject getAndDeleteExact(BinaryObject rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryObject> deleteAll(Collection<BinaryObject> recs) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryObject> deleteAllExact(Collection<BinaryObject> recs) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> R invoke(BinaryObject keyRec, InvokeProcessor<BinaryObject, BinaryObject, R> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> Map<BinaryObject, R> invokeAll(
        Collection<BinaryObject> keyRecs,
        InvokeProcessor<BinaryObject, BinaryObject, R> proc
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder binaryBuilder() {
        return null;
    }

    /**
     * Converts user binary object to row.
     *
     * @param o Binary object.
     * @return Row.
     */
    private Row toKeyRow(BinaryObject o) {
        return null;
    }
}
