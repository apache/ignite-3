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

package org.apache.ignite.internal.pagememory.freelist;

import java.util.Collection;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.util.PageHandler;

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
