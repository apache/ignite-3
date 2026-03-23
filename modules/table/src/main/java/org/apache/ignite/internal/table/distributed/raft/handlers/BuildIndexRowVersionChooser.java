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

package org.apache.ignite.internal.table.distributed.raft.handlers;

import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.util.Cursor;

/** Obtains versions of a row to be indexed while building an index. */
class BuildIndexRowVersionChooser {
    private final PartitionDataStorage storage;

    /** Timestamp of activation of the catalog version in which the index was created. */
    private final long createIndexActivationTs;

    /** Timestamp of activation of the catalog version in which the index start building (get {@link CatalogIndexStatus#BUILDING}). */
    private final long startBuildingIndexActivationTs;

    private final Set<UUID> abortedTransactionIds;

    /**
     * Constructor.
     *
     * @param storage Multi-version partition storage.
     * @param createIndexActivationTs Timestamp of activation of the catalog version in which the index created.
     * @param startBuildingIndexActivationTs Timestamp of activation of the catalog version in which the index start building
     *      (get {@link CatalogIndexStatus#BUILDING}).
     * @param abortedTransactionIds IDs of transactions that are known to have been aborted.
     */
    BuildIndexRowVersionChooser(
            PartitionDataStorage storage,
            long createIndexActivationTs,
            long startBuildingIndexActivationTs,
            Set<UUID> abortedTransactionIds
    ) {
        this.storage = storage;
        this.createIndexActivationTs = createIndexActivationTs;
        this.startBuildingIndexActivationTs = startBuildingIndexActivationTs;
        this.abortedTransactionIds = Set.copyOf(abortedTransactionIds);
    }

    /**
     * Collects binary versions of a row to build an index.
     *
     * <p>Index selection algorithm:</p>
     * <ul>
     *     <li>For writeCommitted with commitTs > activationTs(BUILDING), we will ignore.</li>
     *     <li>For writeCommitted with commitTs <= activationTs(BUILDING), we will take the latest of them.</li>
     *     <li>For writeIntent with beginTs >= activationTs(REGISTERED), we will ignore.</li>
     *     <li>For writeIntent with beginTs < activationTs(REGISTERED), we take.</li>
     * </ul>
     *
     * @param rowId Row ID of interest.
     */
    List<BinaryRowAndRowId> chooseForBuildIndex(RowId rowId) {
        try (Cursor<ReadResult> rowVersionCursor = storage.scanVersions(rowId)) {
            List<BinaryRowAndRowId> result = new ArrayList<>();

            boolean takenLatestVersionOfWriteCommitted = false;

            // Versions are iterated in the newest-to-oldest order.
            for (ReadResult readResult : rowVersionCursor) {
                if (readResult.isEmpty()) {
                    continue;
                }

                if (readResult.isWriteIntent()) {
                    if (beginTs(readResult) >= createIndexActivationTs
                            || abortedTransactionIds.contains(readResult.transactionId())) {
                        continue;
                    }
                } else {
                    if (commitTs(readResult) > startBuildingIndexActivationTs) {
                        continue;
                    } else {
                        takenLatestVersionOfWriteCommitted = true;
                    }
                }

                result.add(new BinaryRowAndRowId(readResult.binaryRow(), rowId));

                if (takenLatestVersionOfWriteCommitted) {
                    break;
                }
            }

            return result;
        }
    }

    private static long beginTs(ReadResult readResult) {
        return beginTimestamp(readResult.transactionId()).longValue();
    }

    private static long commitTs(ReadResult readResult) {
        return readResult.commitTimestamp().longValue();
    }
}
