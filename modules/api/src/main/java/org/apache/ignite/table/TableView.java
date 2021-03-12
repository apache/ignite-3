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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * Table view interface.
 *
 * @param <R> Table row type.
 */
public interface TableView<R> {
    /**
     * Return row from table for given key columns.
     *
     * @param keyRow Row with key columns set.
     * @return Row with all columns filled from the table.
     */
    public R get(R keyRow);

    /**
     * Fills given rows with the values from the table.
     *
     * @param keyRows Rows with key columns set.
     * @return Rows with all columns filled from the table.
     */
    public Collection<R> getAll(Collection<R> keyRows);

    /**
     * Inserts new row into the table if it is not exists or replace existed one.
     *
     * @param row Row to be inserted into table.
     * @return {@code True} if was successful, {@code false} otherwise.
     */
    public boolean upsert(R row);

    /**
     * Inserts new row into the table if it is not exists or replace existed one.
     *
     * @param rows Rows to be inserted into table.
     */
    public void upsertAll(Collection<R> rows);

    /**
     * Insert row into the table and return previous row.
     *
     * @param row Row to be inserted into table.
     * @return Row that was replaced, {@code null} otherwise.
     */
    public R getAndUpsert(R row);

    /**
     * Inserts row into the table if not exists.
     *
     * @param row Row to be inserted into table.
     * @return {@code True} if was successful, {@code false} otherwise.
     */
    public boolean insert(R row);

    /**
     * Inserts rows into the table that are not exists, otherwise skips.
     *
     * @param rows Rows to be inserted into table.
     * @return Rows with key columns set that were not inserted.
     */
    public Collection<R> insertAll(Collection<R> rows);

    /**
     * Replaces an existed row in the table with the given one.
     *
     * @param row Row to replace with.
     * @return {@code True} if the row replaced successfully, {@code false} otherwise.
     */
    public boolean replace(R row);

    /**
     * Replaces an expected row in the table with the new one.
     *
     * @param oldRow Row to be replaced in table.
     * @param newRow Row to replace with.
     * @return {@code True} if the row replaced successfully, {@code false} otherwise.
     */
    public boolean replace(R oldRow, R newRow);

    /**
     * Replaces an existed row in the table and return the replaced one.
     *
     * @param row Row to be inserted into table.
     * @return Row that was replaced with given one, {@code null} otherwise.
     */
    public R getAndReplace(R row);

    /**
     * Remove row from table.
     *
     * @param keyRow Row with key columns set.
     * @return {@code True} if row was successfully removed, {@code  false} otherwise.
     */
    public boolean delete(R keyRow);

    /**
     * Remove exact row from table.
     *
     * @param oldRow Row to be removed.
     * @return {@code True} if row was successfully removed, {@code  false} otherwise.
     */
    public boolean deleteExact(R oldRow);

    /**
     * Remove row from table.
     *
     * @param row Row with key columns set.
     * @return Row that was removed, {@code null} otherwise.
     */
    public R getAndDelete(R row);

    /**
     * Remove exact row from table.
     *
     * @param row Row.
     * @return Row that was removed, {@code null} otherwise.
     */
    public R getAndDeleteExact(R row);

    /**
     * Remove rows from table.
     *
     * @param rows Rows with key columns set.
     * @return Rows with key columns set that were not deleted.
     */
    public Collection<R> deleteAll(Collection<R> rows);

    /**
     * Remove exact rows from table.
     *
     * @param rows Rows with all columns set.
     * @return Rows with key columns set that were not deleted.
     */
    public Collection<R> deleteAllExact(Collection<R> rows);

    /**
     * Invokes an InvokeProcessor against the associated row.
     *
     * @param keyRow Row with key columns set.
     * @return Results of the processing.
     */
    public <T extends Serializable> T invoke(R keyRow, InvokeProcessor<R, R, T> proc);

    /**
     * Invokes an InvokeProcessor against the associated rows.
     *
     * @param keyRows Ordered collection of rows with key columns set.
     * @return Results of the processing.
     */
    public <T extends Serializable> Map<R, T> invokeAll(Collection<R> keyRows, InvokeProcessor<R, R, T> proc);
}
