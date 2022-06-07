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

package org.apache.ignite.sql;

import java.util.Iterator;
import org.jetbrains.annotations.Nullable;

/**
 * SQL result set provides methods to access SQL query result represented as collection of {@link SqlRow}.
 *
 * <p>All the rows in result set have the same structure described in {@link ResultSetMetadata}.
 * ResultSet must be closed after usage to free resources.
 *
 * <p>The class and his methods are not thread-safe. If more than one thread use the result set object
 * please use external synchronization on iterator methods.
 *
 * <p>Note: one and only one of following is possible: {@link #hasRowSet()} returns {@code true}, or {@link #wasApplied()} returns
 * {@code true}, or {@link #affectedRows()} return zero or higher value.
 */
public interface ResultSet extends Iterator<SqlRow>, AutoCloseable {
    /**
     * Returns metadata for the results if the result contains rows ({@link #hasRowSet()} returns {@code true}).
     *
     * @return ResultSet metadata or {@code null} if inapplicable.
     */
    @Nullable ResultSetMetadata metadata();

    /**
     * Returns whether the result of the query execution is a collection of rows, or not.
     *
     * <p>Note: when returns {@code false}, then calling {@link #hasNext()}, {@link #next()} will fail,
     * and either {@link #affectedRows()} return number
     * of affected rows or {@link #wasApplied()} returns {@code true}.
     *
     * @return {@code True} if the query returns rows, {@code false} otherwise.
     */
    boolean hasRowSet();

    /**
     * Returns number of rows affected by the DML statement execution (such as "INSERT", "UPDATE", etc.),
     * or {@code 0} if statement return nothing (such as "ALTER TABLE", etc) or {@code -1} if inapplicable.
     *
     * <p>Note: when returns {@code -1}, then either {@link #hasRowSet()} or {@link #wasApplied()} returns {@code true}.
     *
     * @return Number of rows affected by the query, or {@code 0} if statement return nothing, or {@code -1} if inapplicable.
     */
    long affectedRows();

    /**
     * Returns whether the query that produce this result was a conditional query, or not. E.g. for the query "Create table if not exists"
     * the method returns {@code true} when an operation was applied successfully, and {@code false} when an operation was ignored due to
     * table was already existed.
     *
     * <p>Note: when returns {@code false}, then either {@link #affectedRows()} return number of affected rows or {@link #hasRowSet()}
     * returns {@code true}.
     *
     * @return {@code True} if conditional query applied, {@code false} otherwise.
     */
    boolean wasApplied();

    /**
     * Invalidates result set and cleanup remote resources.
     */
    @Override
    void close();
}
