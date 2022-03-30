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

package org.apache.ignite.internal.idx;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.schemas.table.TableIndexChange;
import org.apache.ignite.internal.idx.event.IndexEvent;
import org.apache.ignite.internal.idx.event.IndexEventParameters;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * Internal index manager facade provides low-level methods for indexes operations.
 */
public interface IndexManager extends Producer<IndexEvent, IndexEventParameters> {
    /**
     * Creates a new index with the specified name.
     *
     * @param idxCanonicalName Index canonical name.
     * @param tblCanonicalName Table canonical name.
     * @param idxChange        Index configuration.
     * @return Index.
     * @throws IndexAlreadyExistsException if the index exists.
     */
    InternalSortedIndex createIndex(
            String idxCanonicalName,
            String tblCanonicalName,
            Consumer<TableIndexChange> idxChange
    );

    /**
     * Create index asynchronously.
     *
     * @param idxCanonicalName Index canonical name.
     * @param tblCanonicalName Table canonical name.
     * @param idxChange        Index configuration.
     * @return Index future, that may be completed exceptionally with {@link IndexAlreadyExistsException} if the index exists.
     */
    CompletableFuture<InternalSortedIndex> createIndexAsync(
            String idxCanonicalName,
            String tblCanonicalName,
            Consumer<TableIndexChange> idxChange
    );

    /**
     * Drop index.
     *
     * @param idxCanonicalName Index canonical name.
     * @throws IndexAlreadyExistsException if the index doesn't exist.
     */
    void dropIndex(String idxCanonicalName);

    /**
     * Drop index asynchronously.
     *
     * @param idxCanonicalName Index canonical name.
     * @return Index future, that may be completed exceptionally with {@link IndexNotFoundException} if the index doesn't exist.
     */
    CompletableFuture<Void> dropIndexAsync(String idxCanonicalName);

    /**
     * Gets indexes of the table.
     *
     * @param tblId Table identifier to lookup indexes.
     * @return Indexes of the table.
     * @throws NodeStoppingException If an implementation stopped before the method was invoked.
     */
    List<InternalSortedIndex> indexes(UUID tblId);

    /**
     * Gets index of the table.
     *
     * @return Index of the table.
     * @throws NodeStoppingException If an implementation stopped before the method was invoked.
     */
    InternalSortedIndex index(String idxCanonicalName);

    /**
     * Gets index of the table asynchronously.
     *
     * @return Index future.
     * @throws NodeStoppingException If an implementation stopped before the method was invoked.
     */
    CompletableFuture<InternalSortedIndex> indexAsync(String idxCanonicalName);

    /**
     * Returns index by specified id.
     *
     * @param id Index id.
     * @return Index or {@code null} if can`t be found.
     */
    @Nullable InternalSortedIndex getIndexById(UUID id);
}
