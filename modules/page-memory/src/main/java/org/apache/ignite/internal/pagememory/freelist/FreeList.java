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

package org.apache.ignite.internal.pagememory.freelist;

import java.util.Collection;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;

/**
 * Free list.
 */
public interface FreeList<T extends Storable> {
    /**
     * Inserts a row.
     *
     * @param row Row.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteInternalCheckedException If failed.
     */
    void insertDataRow(T row, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException;

    /**
     * Inserts rows.
     *
     * @param rows Rows.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteInternalCheckedException If failed.
     */
    void insertDataRows(Collection<T> rows, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException;

    /**
     * Makes an in-place update of a row identified by the link.
     * This has a couple of restrictions:
     * 1. The size of the payload must not change, otherwise the page will be broken (and next insertion will fail due
     * to assertion failure).
     * 2. The row cannot be fragmented. If it is, this will return {@code false} without doing anything.
     *
     * @param link Row link.
     * @param row New row data.
     * @param statHolder Statistics holder to track IO operations.
     * @return {@code True} if was able to update row.
     * @throws IgniteInternalCheckedException If failed.
     */
    boolean updateDataRow(long link, T row, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException;

    /**
     * Updates a row by link.
     *
     * @param link Row link.
     * @param pageHnd Page handler.
     * @param arg Handler argument.
     * @param <S> Argument type.
     * @param <R> Result type.
     * @param statHolder Statistics holder to track IO operations.
     * @return Result.
     * @throws IgniteInternalCheckedException If failed.
     */
    <S, R> R updateDataRow(
            long link,
            PageHandler<S, R> pageHnd,
            S arg,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException;

    /**
     * Removes a row by link.
     *
     * @param link Row link.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteInternalCheckedException If failed.
     */
    void removeDataRowByLink(long link, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException;

    /**
     * Dump statistics to log.
     *
     * @param log Logger.
     */
    void dumpStatistics(IgniteLogger log);
}
