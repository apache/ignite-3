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

package org.apache.ignite.internal.table.distributed.replicator;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.storage.RowId;

/**
 * A container for rows that were inserted, updated or removed.
 */
public class PendingRows {

    /** Rows that were inserted, updated or removed. All row IDs are sorted in natural order to prevent deadlocks upon commit/abort. */
    private final Map<UUID, SortedSet<RowId>> txsPendingRowIds = new ConcurrentHashMap<>();

    /**
     * Adds row ID to the collection of pending rows.
     *
     * @param txId Transaction ID.
     * @param rowId Row ID.
     */
    public void addPendingRowId(UUID txId, RowId rowId) {
        // We are not using computeIfAbsent here because we want the lambda to be executed atomically.
        txsPendingRowIds.compute(txId, (k, v) -> {
            if (v == null) {
                v = new TreeSet<>();
            }

            v.add(rowId);

            return v;
        });
    }

    /**
     * Adds row IDs to the collection of pending rows.
     *
     * @param txId Transaction ID.
     * @param rowIds Row IDs.
     */
    public void addPendingRowIds(UUID txId, Collection<RowId> rowIds) {
        // We are not using computeIfAbsent here because we want the lambda to be executed atomically.
        txsPendingRowIds.compute(txId, (k, v) -> {
            if (v == null) {
                v = new TreeSet<>();
            }

            v.addAll(rowIds);

            return v;
        });
    }

    /**
     * Removes all row IDs for the given transaction.
     *
     * @param txId Transaction ID.
     */
    public void removePendingRowIds(UUID txId, Set<RowId> pendingRowIds) {
        txsPendingRowIds.computeIfPresent(txId, (k, v) -> {
            v.removeAll(pendingRowIds);

            return v.isEmpty() ? null : v;
        });
    }

    /**
     * Returns pending row IDs for the given transaction or an empty set if there are no pending rows.
     *
     * @param txId Transaction ID.
     * @return Pending row IDs.
     */
    public Set<RowId> getPendingRowIds(UUID txId) {
        Set<RowId> pendingRows = new TreeSet<>();

        txsPendingRowIds.computeIfPresent(txId, (k, v) -> {
            pendingRows.addAll(v);

            return v;
        });

        return pendingRows;
    }

}
