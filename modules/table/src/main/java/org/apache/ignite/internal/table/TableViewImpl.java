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

import java.util.Collection;
import java.util.List;
import table.BinaryRow;
import table.InvokeProcessor;
import table.KVView;
import table.RecordView;
import table.Table;
import table.binary.BinaryRowBuilder;
import table.mapper.KeyMapper;
import table.mapper.RecordMapper;
import table.mapper.ValueMapper;

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
    @Override public <K> BinaryRow get(K keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K> Collection<BinaryRow> getAll(List<K> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean upsert(BinaryRow row) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(List<BinaryRow> recs) {

    }

    /** {@inheritDoc} */
    @Override public boolean insert(BinaryRow row) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void insertAll(List<BinaryRow> recs) {

    }

    /** {@inheritDoc} */
    @Override public BinaryRow getAndUpsert(BinaryRow rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(BinaryRow rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(BinaryRow oldRec, BinaryRow newRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public BinaryRow getAndReplace(BinaryRow rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K> boolean delete(K keyRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(BinaryRow oldRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <K> BinaryRow getAndDelete(K rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public BinaryRow getAndDeleteExact(BinaryRow rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K> void deleteAll(List<K> recs) {

    }

    /** {@inheritDoc} */
    @Override public void deleteAllExact(List<BinaryRow> recs) {

    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryRow> selectBy(Criteria<BinaryRow> c) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void deleteBy(Criteria<BinaryRow> c) {

    }

    /** {@inheritDoc} */
    @Override public <K, T> T invoke(K keyRec, InvokeProcessor<BinaryRow, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, T> T invokeAll(List<K> keyRecs, InvokeProcessor<BinaryRow, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public BinaryRowBuilder binaryRowBuilder(Object... args) {
//        BinaryRow row = null;

//        TableSchema schema = table.schemaManager().schema();
//        assert args.length == schema.keyColumns().length();

//        for (int i = 0; i < args.length; i++)
//            row.setColumn(i, args[i]);

//        return row;

        return null;
    }
}
