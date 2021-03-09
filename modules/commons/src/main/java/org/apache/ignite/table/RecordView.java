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

package org.apache.ignite.table;

import java.util.Collection;
import java.util.List;

/**
 * Record adapter for Table.
 */
public interface RecordView<R> {
    /**
     * Return record from table for given key fields.
     *
     * @param keyRec Record with key fields set.
     * @return Record with all fields filled from the table.
     */
    public <K> R get(K keyRec);

    /**
     * Fills given record with the values from the table.
     * Similar to {@link #get(Object)}, but return original object with filled value fields.
     *
     * @param recObjToFill Record object with key fields to be filled.
     * @return Record with all fields filled from the table.
     */
    public R fill(R recObjToFill);

    /**
     * Fills given records with the values from the table.
     *
     * @param keyRecs Records with key fields set.
     * @return Records with all fields filled from the table.
     */
    public <K> Collection<R> getAll(List<K> keyRecs);

    /**
     * Inserts new record into the table if it is not exists or replace existed one.
     *
     * @param rec Record to be inserted into table.
     * @return {@code True} if was successful, {@code false} otherwise.
     */
    public boolean upsert(R rec);

    /**
     * Inserts new record into the table if it is not exists or replace existed one.
     *
     * @param recs Records to be inserted into table.
     */
    public void upsertAll(List<R> recs);

    /**
     * Inserts record into the table if not exists.
     *
     * @param rec Record to be inserted into table.
     * @return {@code True} if was successful, {@code false} otherwise.
     */
    public boolean insert(R rec);

    /**
     * Inserts records into the table that are not exists, otherwise skips.
     *
     * @param recs Records to be inserted into table.
     */
    public void insertAll(List<R> recs);

    /**
     * Insert record into the table and return previous record.
     *
     * @param rec Record to be inserted into table.
     * @return Record that was replaced, {@code null} otherwise.
     */
    public R getAndUpsert(R rec);

    /**
     * Replaces an existed record in the table with the given one.
     *
     * @param rec Record to replace with.
     * @return {@code True} if the record replaced successfully, {@code false} otherwise.
     */
    public boolean replace(R rec);

    /**
     * Replaces an expected record in the table with the new one.
     *
     * @param oldRec Record to be replaced in table.
     * @param newRec Record to replace with.
     * @return {@code True} if the record replaced successfully, {@code false} otherwise.
     */
    public boolean replace(R oldRec, R newRec);

    /**
     * Replaces an existed record in the table and return the replaced one.
     *
     * @param rec Record to be inserted into table.
     * @return Record that was replaced with given one, {@code null} otherwise.
     */
    public R getAndReplace(R rec);

    /**
     * Remove record from table.
     *
     * @param keyRec Record with key fields set.
     * @return {@code True} if record was successfully removed, {@code  false} otherwise.
     */
    public <K> boolean delete(K keyRec);

    /**
     * Remove exact record from table.
     *
     * @param oldRec Record to be removed.
     * @return {@code True} if record was successfully removed, {@code  false} otherwise.
     */
    public boolean deleteExact(R oldRec);

    /**
     * Remove record from table.
     *
     * @param rec Record with key fields set.
     * @return Record that was removed, {@code null} otherwise.
     */
    public <K> R getAndDelete(K rec);

    /**
     * Remove exact record from table.
     *
     * @param rec Record.
     * @return Record that was removed, {@code null} otherwise.
     */
    public R getAndDeleteExact(R rec);

    /**
     * Remove records from table.
     *
     * @param recs Records with key fields set.
     */
    public <K> void deleteAll(List<K> recs);

    /**
     * Remove exact records from table.
     *
     * @param recs Records with key fields set.
     */
    public void deleteAllExact(List<R> recs);

    /**
     * Invokes an InvokeProcessor against the associated row.
     *
     * @param keyRec Record with key fields set.
     * @return Results of the processing.
     */
    public <K, T> T invoke(K keyRec, InvokeProcessor<R, T> proc);

    /**
     * Invokes an InvokeProcessor against the associated rows.
     *
     * @param keyRecs Sorted collection of records with key fields set.
     * @return Results of the processing.
     */
    public <K, T> T invokeAll(List<K> keyRecs, InvokeProcessor<R, T> proc);

    //TODO: Hibernate CriteriaAPI, + Hazelcast
    public Collection<R> selectBy(Criteria<R> c);
    public void deleteBy(Criteria<R> c);

    /**
     * Criteria API.
     */
    interface Criteria<R> {

    }
}
