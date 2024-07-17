/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.sql;

import org.apache.ignite.lang.Cursor;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Provides methods to access SQL query results represented by a collection of {@link SqlRow}.
 *
 * <p>All rows in the result set have the same structure described in {@link ResultSetMetadata}.
 * ResultSet must be closed after usage to free resources.
 *
 * <p>The class and its methods are not thread-safe. If more than one thread use the result set object,
 * use external synchronization on iterator methods.
 *
 * <p>Note: Only one of following is possible: {@link #hasRowSet()} returns {@code true}, or {@link #wasApplied()} returns
 * {@code true}, or {@link #affectedRows()} return zero or higher value.
 *
 * @param <T> A type of the objects contained by this result set (when row set is present). This will be either {@link SqlRow}
 *     if no explicit mapper is provided or a particular type defined by supplied mapper.
 *
 * @see IgniteSql#execute(Transaction, String, Object...)
 * @see IgniteSql#execute(Transaction, Mapper, String, Object...)
 */
public interface ResultSet<T> extends Cursor<T> {
    /**
     * Returns metadata for the results if the result contains rows (if {@link #hasRowSet()} returns {@code true}).
     *
     * @return ResultSet metadata or {@code null} if not applicable.
     */
    @Nullable ResultSetMetadata metadata();

    /**
     * Indicates whether the query result is a collection of rows.
     *
     * <p>Note: If the method returns {@code false}, calling {@link #hasNext()}, {@link #next()} will fail,
     * and either {@link #affectedRows()} returns the number
     * of the affected rows or {@link #wasApplied()} returns {@code true}.
     *
     * @return {@code True} if the query returns rows, {@code false} otherwise.
     */
    boolean hasRowSet();

    /**
     * Returns the number of rows affected by the DML statement execution (such as "INSERT", "UPDATE", etc.),
     * or {@code 0} if the statement returns nothing (such as "ALTER TABLE", etc.), or {@code -1} if not applicable.
     *
     * <p>Note: If the method returns {@code -1}, either {@link #hasRowSet()} or {@link #wasApplied()} returns {@code true}.
     *
     * @return Number of rows affected by the query, or {@code 0} if statement return nothing, or {@code -1} if not applicable.
     */
    long affectedRows();

    /**
     * Indicates whether the query that had produced the result was a conditional query.
     * E.g., for query "Create table if not exists", the method returns {@code true} if
     * the operation was successful, and {@code false} if the operation was ignored becasue
     * the table already existed.
     *
     * <p>Note: If the method returns {@code false}, either {@link #affectedRows()} returns
     * the number the affected rows or {@link #hasRowSet()} returns {@code true}.
     *
     * @return {@code True} if the query is conditional, {@code false} otherwise.
     */
    boolean wasApplied();

    /**
     * Invalidates result set and cleans up remote resources.
     */
    @Override
    void close();
}
