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
 * Table view interface.
 * @param <R> Table row type.
 */
public interface TableView<R> {
    /**
     * Return row from table for given key fields.
     *
     * @param keyRec Row with key fields set.
     * @return Row with all fields filled from the table.
     */
    public <K> R get(K keyRec);

    /**
     * Fills given rows with the values from the table.
     *
     * @param keyRecs Rows with key fields set.
     * @return Rows with all fields filled from the table.
     */
    public <K> Collection<R> getAll(List<K> keyRecs);

    /**
     * Inserts new row into the table if it is not exists or replace existed one.
     *
     * @param rec Row to be inserted into table.
     * @return {@code True} if was successful, {@code false} otherwise.
     */
    public boolean upsert(R rec);

    /**
     * Inserts new row into the table if it is not exists or replace existed one.
     *
     * @param recs Rows to be inserted into table.
     */
    public void upsertAll(List<R> recs);

    /**
     * Insert row into the table and return previous row.
     *
     * @param rec Row to be inserted into table.
     * @return Row that was replaced, {@code null} otherwise.
     */
    public R getAndUpsert(R rec);

    /**
     * Inserts row into the table if not exists.
     *
     * @param rec Row to be inserted into table.
     * @return {@code True} if was successful, {@code false} otherwise.
     */
    public boolean insert(R rec);

    /**
     * Inserts rows into the table that are not exists, otherwise skips.
     *
     * @param recs Rows to be inserted into table.
     */
    public void insertAll(List<R> recs);

    /**
     * Replaces an existed row in the table with the given one.
     *
     * @param rec Row to replace with.
     * @return {@code True} if the row replaced successfully, {@code false} otherwise.
     */
    public boolean replace(R rec);

    /**
     * Replaces an expected row in the table with the new one.
     *
     * @param oldRec Row to be replaced in table.
     * @param newRec Row to replace with.
     * @return {@code True} if the row replaced successfully, {@code false} otherwise.
     */
    public boolean replace(R oldRec, R newRec);

    /**
     * Replaces an existed row in the table and return the replaced one.
     *
     * @param rec Row to be inserted into table.
     * @return Row that was replaced with given one, {@code null} otherwise.
     */
    public R getAndReplace(R rec);

    /**
     * Remove row from table.
     *
     * @param keyRec Row with key fields set.
     * @return {@code True} if row was successfully removed, {@code  false} otherwise.
     */
    public <K> boolean delete(K keyRec);

    /**
     * Remove exact row from table.
     *
     * @param oldRec Row to be removed.
     * @return {@code True} if row was successfully removed, {@code  false} otherwise.
     */
    public boolean deleteExact(R oldRec);

    /**
     * Remove row from table.
     *
     * @param rec Row with key fields set.
     * @return Row that was removed, {@code null} otherwise.
     */
    public <K> R getAndDelete(K rec);

    /**
     * Remove exact row from table.
     *
     * @param rec Row.
     * @return Row that was removed, {@code null} otherwise.
     */
    public R getAndDeleteExact(R rec);

    /**
     * Remove rows from table.
     *
     * @param recs Rows with key fields set.
     */
    public <K> void deleteAll(List<K> recs);

    /**
     * Remove exact rows from table.
     *
     * @param recs Rows with key fields set.
     */
    public void deleteAllExact(List<R> recs);

    /**
     * Invokes an InvokeProcessor against the associated row.
     *
     * @param keyRec Row with key fields set.
     * @param <T> //TODO: Must be serializable or will be implicitly converted to BinaryObject for transportation to remote node?
     * @return Results of the processing.
     */
    public <K, T> T invoke(K keyRec, InvokeProcessor<R, T> proc);

    /**
     * Invokes an InvokeProcessor against the associated rows.
     *
     * @param keyRecs Sorted collection of rows with key fields set.
     * @return Results of the processing.
     */
    public <K, T> T invokeAll(List<K> keyRecs, InvokeProcessor<R, T> proc);

    // TODO: Do we expect operations over undefined scope using Criteria API in RecordView/Table or move next to ScanQueryAPI?
    public Collection<R> selectBy(Criteria<R> c);

    public void deleteBy(Criteria<R> c);

    /**
     * Criteria API.
     *
     */
    interface Criteria<R> {

    }
}
