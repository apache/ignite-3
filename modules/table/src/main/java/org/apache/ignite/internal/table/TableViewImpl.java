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
import java.util.List;
import java.util.Map;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KV;
import org.apache.ignite.table.KVView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.binary.Row;
import org.apache.ignite.table.binary.RowBuilder;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;

/**
 * Table view implementation provides functionality to access binary rows.
 */
public class TableViewImpl implements Table {
    /** Table. */
    private final TableStorage table;

    /**
     * Constructor.
     *
     * @param table Table.
     */
    public TableViewImpl(TableStorage table) {
        this.table = table;
    }

    /** {@inheritDoc} */
    @Override public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        return new RecordViewImpl<>(table, recMapper);
    }

    /** {@inheritDoc} */
    @Override public <K, V> KVView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        return new KVViewImpl<>(table, keyMapper, valMapper);
    }

    /** {@inheritDoc} */
    @Override public KV kvView() {
        return new KVImpl(table);
    }

    /** {@inheritDoc} */
    @Override public Row get(Row keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Row> getAll(Collection<Row> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean upsert(Row row) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(Collection<Row> recs) {
    }

    /** {@inheritDoc} */
    @Override public boolean insert(Row row) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Collection<Row> insertAll(Collection<Row> recs) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public Row getAndUpsert(Row rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Row rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Row oldRec, Row newRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Row getAndReplace(Row rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean delete(Row keyRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(Row oldRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Row getAndDelete(Row rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Row getAndDeleteExact(Row rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<Row> deleteAll(Collection<Row> recs) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public Collection<Row> deleteAllExact(Collection<Row> recs) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(Row keyRec, InvokeProcessor<Row, Row, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<Row, T> invokeAll(Collection<Row> keyRecs, InvokeProcessor<Row, Row, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public RowBuilder binaryRowBuilder() {
        return null;
    }
}
