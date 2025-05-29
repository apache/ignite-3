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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.util.FastTimestamps;

/**
 * A container for rows that were inserted, updated or removed.
 */
public class PendingRows {

    /** Empty sorted set. */
    private static final SortedSet<RowId> EMPTY_SET = Collections.emptySortedSet();

    /** Rows that were inserted, updated or removed. All row IDs are sorted in natural order to prevent deadlocks upon commit/abort. */
    private final Map<UUID, SortedSet<RowId>> txsPendingRowIds = new ConcurrentHashMap<>();

    private final Map<RowId, PendingRowTxHistory> txHistoryByRowId = new ConcurrentHashMap<>();

    /**
     * Adds row ID to the collection of pending rows.
     *
     * @param txId Transaction ID.
     * @param rowId Row ID.
     */
    public void addPendingRowId(UUID txId, RowId rowId) {
        addPendingRowIds(txId, List.of(rowId));
    }

    /**
     * Adds row IDs to the collection of pending rows.
     *
     * @param txId Transaction ID.
     * @param rowIds Row IDs.
     */
    public void addPendingRowIds(UUID txId, Collection<RowId> rowIds) {
        var historyItem = new PendingRowTxHistoryItem(txId);

        // We are not using computeIfAbsent here because we want the lambda to be executed atomically.
        txsPendingRowIds.compute(txId, (k, v) -> {
            if (v == null) {
                v = new TreeSet<>();
            }

            v.addAll(rowIds);

            return v;
        });

        for (RowId rowId : rowIds) {
            txHistoryByRowId.compute(rowId, (rowId1, pendingRowTxHistory) -> {
                if (pendingRowTxHistory == null) {
                    pendingRowTxHistory = new PendingRowTxHistory();
                }

                pendingRowTxHistory.queue.add(historyItem);

                return pendingRowTxHistory;
            });
        }
    }

    /**
     * Removes all pending row IDs for the given transaction.
     *
     * @param txId Transaction ID.
     * @return Pending row IDs mapped to the provided transaction or an empty set if there were none.
     */
    public Set<RowId> removePendingRowIds(UUID txId) {
        Set<RowId> pendingRows = txsPendingRowIds.remove(txId);

        return pendingRows == null ? EMPTY_SET : pendingRows;
    }

    /** No doc. */
    public Collection<PendingRowTxHistoryItem> pendingRowTxHistory(RowId rowId) {
        PendingRowTxHistory history = txHistoryByRowId.get(rowId);

        return history == null ? List.of() : history.snapshotSorted();
    }

    /** No doc. */
    private static final class PendingRowTxHistory {
        private final Collection<PendingRowTxHistoryItem> queue = new ConcurrentLinkedQueue<>();

        /** No doc. */
        private Collection<PendingRowTxHistoryItem> snapshotSorted() {
            var res = new ArrayList<>(queue);

            res.sort(Comparator.comparing(i -> i.coarseTimeMillis));

            return Collections.unmodifiableCollection(res);
        }
    }

    /** No doc. */
    public static final class PendingRowTxHistoryItem {
        private final long coarseTimeMillis;

        private final long nanoTime;

        private final UUID txId;

        private PendingRowTxHistoryItem(long time, long nanoTime, UUID txId) {
            this.coarseTimeMillis = time;
            this.nanoTime = nanoTime;
            this.txId = txId;
        }

        private PendingRowTxHistoryItem(UUID txId) {
            this(FastTimestamps.coarseCurrentTimeMillis(), System.nanoTime(), txId);
        }

        @Override
        public String toString() {
            return "["
                    + "txId=" + txId
                    + ", coarseTimeMillis=" + coarseTimeMillis
                    + ", nanoTime=" + nanoTime
                    + ']';
        }
    }
}
