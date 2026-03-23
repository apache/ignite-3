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

package org.apache.ignite.internal.storage;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MvPartitionStorage#read} result.
 */
public class ReadResult {
    /** Unset commit partition id value. */
    public static final int UNDEFINED_COMMIT_PARTITION_ID = -1;

    /** ID of the row. */
    private final RowId rowId;

    /** Data. {@code null} iff the result is empty (i.e. no row exists or it is a tombstone). */
    private final @Nullable BinaryRow binaryRow;

    /** Transaction id. Not {@code null} iff this is a write-intent. */
    private final @Nullable UUID transactionId;

    /** Commit zone id. Not {@code null} iff this is a write-intent. */
    private final @Nullable Integer commitZoneId;

    /** Commit partition id. If this is not a write-intent it is equal to {@link #UNDEFINED_COMMIT_PARTITION_ID}. */
    private final int commitPartitionId;

    /**
     * Commit timestamp of this version (if exists). Non-null for committed versions, {@code null} for write intents.
     */
    private final @Nullable HybridTimestamp commitTs;

    /**
     * Timestamp of the newest commit of the data. Not {@code null} if committed version exists, this is a
     * write-intent and read was made with a timestamp.
     * Might be {@code null} for {@link MvPartitionStorage#scanVersions(RowId)} even for write intents having
     * a preceding committed version.
     */
    private final @Nullable HybridTimestamp newestCommitTs;

    private ReadResult(
            RowId rowId,
            @Nullable BinaryRow binaryRow,
            @Nullable UUID transactionId,
            @Nullable Integer commitZoneId,
            @Nullable HybridTimestamp commitTs,
            @Nullable HybridTimestamp newestCommitTs,
            int commitPartitionId
    ) {
        this.rowId = rowId;
        this.binaryRow = binaryRow;

        // If transaction id is not null, then commitZoneId and commitPartitionId should be defined.
        assert (transactionId == null) || (commitZoneId != null && commitPartitionId != -1);

        // If transaction id is null, then commitZoneId and commitPartitionId should not be defined.
        assert (transactionId != null) || (commitZoneId == null && commitPartitionId == -1);

        this.transactionId = transactionId;
        this.commitZoneId = commitZoneId;
        this.commitTs = commitTs;
        this.newestCommitTs = newestCommitTs;
        this.commitPartitionId = commitPartitionId;
    }

    /**
     * Returns an empty read result (that is, a result for a missing row ID or for a tombstone) for the given row ID.
     *
     * @param rowId ID of the row for which to create a ReadResult.
     * @return An empty read result.
     */
    public static ReadResult empty(RowId rowId) {
        return new ReadResult(rowId, null, null, null, null, null, UNDEFINED_COMMIT_PARTITION_ID);
    }

    public static ReadResult createFromWriteIntent(RowId rowId, @Nullable BinaryRow binaryRow, UUID transactionId, int commitZoneId,
            int commitPartitionId, @Nullable HybridTimestamp lastCommittedTimestamp) {
        return new ReadResult(rowId, binaryRow, transactionId, commitZoneId, null, lastCommittedTimestamp, commitPartitionId);
    }

    public static ReadResult createFromCommitted(RowId rowId, @Nullable BinaryRow binaryRow, HybridTimestamp commitTs) {
        return new ReadResult(rowId, binaryRow, null, null, commitTs, null, UNDEFINED_COMMIT_PARTITION_ID);
    }

    /**
     * Returns ID of the corresponding row.
     *
     * @return ID of the corresponding row.
     */
    public RowId rowId() {
        return rowId;
    }

    /**
     * Returns table row representation of the data, {@code null} if {@link #isEmpty()}.
     *
     * @return Table row representation of the data, {@code null} if {@link #isEmpty()}.
     */
    public @Nullable BinaryRow binaryRow() {
        return binaryRow;
    }

    /**
     * Returns transaction id part of the transaction state if this is a write-intent,
     * {@code null} otherwise.
     *
     * @return Transaction id part of the transaction state if this is a write-intent,
     *         {@code null} otherwise.
     */
    public @Nullable UUID transactionId() {
        return transactionId;
    }

    /**
     * Returns commit zone id part of the transaction state if this is a write-intent,
     * {@code null} otherwise.
     *
     * @return Commit zone id part of the transaction state if this is a write-intent,
     *         {@code null} otherwise.
     */
    public @Nullable Integer commitZoneId() {
        return commitZoneId;
    }

    /**
     * Returns commit timestamp of this version (of exists). Non-null for committed versions, {@code null} for write intents.
     *
     * @return Commit timestamp of this version (of exists). Non-null for committed versions, {@code null} for write intents.
     */
    public @Nullable HybridTimestamp commitTimestamp() {
        return commitTs;
    }

    /**
     * Returns timestamp of the most recent commit of the row. Not {@code null} if committed version exists, this is a
     * write-intent and read was made with a timestamp. Might be {@code null} for {@link MvPartitionStorage#scanVersions(RowId)}
     * even for write intents having a preceding committed version.
     *
     * @return Timestamp of the most recent commit of the row.
     */
    public @Nullable HybridTimestamp newestCommitTimestamp() {
        return newestCommitTs;
    }

    /**
     * Returns commit partition id part of the transaction state if this is a write-intent,
     * {@link #UNDEFINED_COMMIT_PARTITION_ID} otherwise.
     *
     * @return Commit partition id part of the transaction state if this is a write-intent,
     *         {@link #UNDEFINED_COMMIT_PARTITION_ID} otherwise.
     */
    public int commitPartitionId() {
        return commitPartitionId;
    }

    public boolean isWriteIntent() {
        return transactionId != null;
    }

    public boolean isEmpty() {
        return binaryRow == null;
    }
}
