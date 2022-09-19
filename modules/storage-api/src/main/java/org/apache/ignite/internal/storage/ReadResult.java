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
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MvPartitionStorage#read} result.
 */
public class ReadResult {
    /** Unset commit partition id value. */
    public static final int UNDEFINED_COMMIT_PARTITION_ID = -1;

    /** Empty read result. */
    private static final ReadResult EMPTY = new ReadResult(null, null, null, null, UNDEFINED_COMMIT_PARTITION_ID);

    /** Data. */
    private final @Nullable BinaryRow binaryRow;

    /** Transaction id. Not {@code null} iff this is a write-intent. */
    private final @Nullable UUID transactionId;

    /** Commit table id. Not {@code null} iff this is a write-intent. */
    private final @Nullable UUID commitTableId;

    /** Commit table id. If this is not a write-intent it is equal to {@link #UNDEFINED_COMMIT_PARTITION_ID}. */
    private final int commitPartitionId;

    /**
     * Timestamp of the newest commit of the data. Not {@code null} iff committed version exists, this is a
     * write-intent and read was made with a timestamp.
     */
    private final @Nullable HybridTimestamp newestCommitTs;

    private ReadResult(BinaryRow binaryRow, @Nullable UUID transactionId, @Nullable UUID commitTableId,
            @Nullable HybridTimestamp newestCommitTs, int commitPartitionId) {
        this.binaryRow = binaryRow;

        // If transaction is not null, then commitTableId and commitPartitionId should be defined.
        assert (transactionId == null) || (commitTableId != null && commitPartitionId != -1);

        // If transaction id is null, then commitTableId and commitPartitionId should not be defined.
        assert (transactionId != null) || (commitTableId == null && commitPartitionId == -1);

        this.transactionId = transactionId;
        this.commitTableId = commitTableId;
        this.newestCommitTs = newestCommitTs;
        this.commitPartitionId = commitPartitionId;
    }

    public static ReadResult createFromWriteIntent(BinaryRow binaryRow, UUID transactionId, UUID commitTableId,
            @Nullable HybridTimestamp lastCommittedTimestamp, int commitPartitionId) {
        return new ReadResult(binaryRow, transactionId, commitTableId, lastCommittedTimestamp, commitPartitionId);
    }

    public static ReadResult createFromCommitted(BinaryRow binaryRow) {
        return new ReadResult(binaryRow, null, null, null, UNDEFINED_COMMIT_PARTITION_ID);
    }

    public static ReadResult empty() {
        return EMPTY;
    }

    /**
     * Returns binary row representation of the data.
     *
     * @return Binary row representation of the data.
     */
    public BinaryRow binaryRow() {
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
     * Returns commit table id part of the transaction state if this is a write-intent,
     * {@code null} otherwise.
     *
     * @return Commit table id part of the transaction state if this is a write-intent,
     *         {@code null} otherwise.
     */
    public @Nullable UUID commitTableId() {
        return commitTableId;
    }

    /**
     * Returns timestamp of the most recent commit of the row.
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
}
