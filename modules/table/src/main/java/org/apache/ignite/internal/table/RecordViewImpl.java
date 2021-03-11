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
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.schema.marshaller.Marshaller;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.binary.BinaryRow;
import org.apache.ignite.table.mapper.RecordMapper;

/**
 * Record view implementation provides functionality to access table
 * transparently map user defined class to binary row and vice versa.
 *
 * @param <R> Record type.
 */
public class RecordViewImpl<R> implements RecordView<R> {
    /** Table */
    private final TableStorage table;

    /**
     * Constructor.
     *
     * @param table Table.
     * @param mapper Record class mapper.
     */
    public RecordViewImpl(TableStorage table, RecordMapper<R> mapper) {
        this.table = table;
    }

    /** {@inheritDoc} */
    @Override public <K> R get(K recObjToFill) {
        Marshaller marsh = marshaller();

        BinaryRow kRow = marsh.toKeyRow(recObjToFill);

        BinaryRow tRow = table.get(kRow);

        return marsh.unmarshallToRecord(tRow);
    }

    /** {@inheritDoc} */
    @Override public R fill(R recObjToFill) {
        Marshaller marsh = marshaller();

        BinaryRow kRow = marsh.toKeyRow(recObjToFill);

        BinaryRow tRow = table.get(kRow);

        return marsh.unmarshallToRecord(recObjToFill, tRow);
    }

    /** {@inheritDoc} */
    @Override public <K> List<R> getAll(List<K> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean insert(R rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void insertAll(List<R> recs) {

    }

    /** {@inheritDoc} */
    @Override public boolean upsert(R rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(List<R> recs) {

    }

    /** {@inheritDoc} */
    @Override public R getAndUpsert(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(R rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(R oldRec, R newRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public R getAndReplace(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K> boolean delete(K keyRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(R oldRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <K> R getAndDelete(K rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K> void deleteAll(List<K> recs) {

    }

    /** {@inheritDoc} */
    @Override public R getAndDeleteExact(R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void deleteAllExact(List<R> recs) {

    }

    /** {@inheritDoc} */
    @Override public Collection<R> selectBy(Criteria<R> template) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public void deleteBy(Criteria<R> c) {
    }

    /** {@inheritDoc} */
    @Override public <K, T> T invoke(K keyRec, InvokeProcessor<K, R, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, T> T invokeAll(List<K> keyRecs, InvokeProcessor<K, R, T> proc) {
        return null;
    }

    /**
     * @return Marshaller.
     */
    private Marshaller marshaller() {
        return null;        // table.schemaManager().marshaller();
    }

}
