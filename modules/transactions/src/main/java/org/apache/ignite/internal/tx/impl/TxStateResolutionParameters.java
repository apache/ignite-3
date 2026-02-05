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

package org.apache.ignite.internal.tx.impl;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.tx.impl.PlacementDriverHelper.AWAIT_PRIMARY_REPLICA_TIMEOUT;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tx.message.RowIdMessage;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxStateCommitPartitionRequest;
import org.jetbrains.annotations.Nullable;

/**
 * Parameters for transaction state resolution request.
 */
public class TxStateResolutionParameters {
    private final UUID txId;

    private final ZonePartitionId commitGroupId;

    private final HybridTimestamp readTimestamp;

    @Nullable
    private final Long senderCurrentConsistencyToken;

    @Nullable
    private final ZonePartitionId senderGroupId;

    @Nullable
    private final HybridTimestamp newestCommitTimestamp;

    @Nullable
    private final RowId rowId;

    private final long awaitCommitPartitionAvailabilityTimeout;

    private final TimeUnit awaitCommitPartitionAvailabilityTimeUnit;

    private TxStateResolutionParameters(
            UUID txId,
            ZonePartitionId commitGroupId,
            HybridTimestamp readTimestamp,
            @Nullable Long senderCurrentConsistencyToken,
            @Nullable ZonePartitionId senderGroupId,
            @Nullable HybridTimestamp newestCommitTimestamp,
            @Nullable RowId rowId,
            long awaitCommitPartitionAvailabilityTimeout,
            TimeUnit awaitCommitPartitionAvailabilityTimeUnit
    ) {
        this.txId = txId;
        this.commitGroupId = commitGroupId;
        this.readTimestamp = readTimestamp;
        this.senderCurrentConsistencyToken = senderCurrentConsistencyToken;
        this.senderGroupId = senderGroupId;
        this.newestCommitTimestamp = newestCommitTimestamp;
        this.rowId = rowId;
        this.awaitCommitPartitionAvailabilityTimeout = awaitCommitPartitionAvailabilityTimeout;
        this.awaitCommitPartitionAvailabilityTimeUnit = awaitCommitPartitionAvailabilityTimeUnit;
    }

    /**
     * Transaction ID.
     *
     * @return Transaction ID.
     */
    public UUID txId() {
        return txId;
    }

    /**
     * Commit group ID.
     *
     * @return Commit group ID.
     */
    public ZonePartitionId commitGroupId() {
        return commitGroupId;
    }

    /**
     * Read timestamp.
     *
     * @return Read timestamp.
     */
    public HybridTimestamp readTimestamp() {
        return readTimestamp;
    }

    /**
     * Sender's current consistency token. See {@link TxStateCommitPartitionRequest#senderCurrentConsistencyToken()}.
     *
     * @return Sender's current consistency token.
     */
    public @Nullable Long senderCurrentConsistencyToken() {
        return senderCurrentConsistencyToken;
    }

    /**
     * Sender's group ID. See {@link TxStateCommitPartitionRequest#senderGroupId()}.
     *
     * @return Sender's group ID.
     */
    public @Nullable ZonePartitionId senderGroupId() {
        return senderGroupId;
    }

    /**
     * Newest commit timestamp known to the sender. Should NOT be {@code null} if present and {@link #rowId} is not {@code null}.
     *
     * @return Newest commit timestamp known to the sender.
     */
    public @Nullable HybridTimestamp newestCommitTimestamp() {
        return newestCommitTimestamp;
    }

    /**
     * Row ID.
     *
     * @return Row ID.
     */
    public @Nullable RowId rowId() {
        return rowId;
    }

    /**
     * Creates a {@link RowIdMessage} from the {@link #rowId()}.
     *
     * @param txMessagesFactory Transaction messages factory.
     * @return Row ID message or {@code null} if {@link #rowId()} is {@code null}.
     */
    public @Nullable RowIdMessage rowIdMessage(TxMessagesFactory txMessagesFactory) {
        return rowId == null
                ? null
                : txMessagesFactory.rowIdMessage().uuid(rowId.uuid()).partitionId(rowId.partitionId()).build();
    }

    /**
     * Get await commit partition availability time unit.
     *
     * @return Time unit.
     */
    public TimeUnit awaitCommitPartitionAvailabilityTimeUnit() {
        return awaitCommitPartitionAvailabilityTimeUnit;
    }

    /**
     * Get await commit partition availability timeout.
     *
     * @return Timeout.
     */
    public long awaitCommitPartitionAvailabilityTimeout() {
        return awaitCommitPartitionAvailabilityTimeout;
    }

    public static TxStateResolutionTxStateResolutionParametersBuilder txStateResolutionParameters() {
        return new TxStateResolutionTxStateResolutionParametersBuilder();
    }

    public static class TxStateResolutionTxStateResolutionParametersBuilder {
        private UUID txId;
        private ZonePartitionId commitGroupId;
        private HybridTimestamp readTimestamp = HybridTimestamp.MIN_VALUE;
        private Long senderCurrentConsistencyToken;
        private ZonePartitionId senderGroupId;
        private HybridTimestamp newestCommitTimestamp;
        private RowId rowId;
        private Long awaitCommitPartitionAvailabilityTimeout = (long) AWAIT_PRIMARY_REPLICA_TIMEOUT;
        private TimeUnit awaitCommitPartitionAvailabilityTimeUnit = TimeUnit.SECONDS;

        public TxStateResolutionTxStateResolutionParametersBuilder txId(UUID txId) {
            this.txId = txId;

            return this;
        }

        public TxStateResolutionTxStateResolutionParametersBuilder commitGroupId(ZonePartitionId commitGroupId) {
            this.commitGroupId = commitGroupId;

            return this;
        }

        public TxStateResolutionTxStateResolutionParametersBuilder readTimestamp(HybridTimestamp readTimestamp) {
            if (readTimestamp != null) {
                this.readTimestamp = readTimestamp;
            }

            return this;
        }

        public TxStateResolutionTxStateResolutionParametersBuilder senderGroupId(ZonePartitionId senderGroupId) {
            this.senderGroupId = senderGroupId;

            return this;
        }

        public TxStateResolutionTxStateResolutionParametersBuilder senderCurrentConsistencyToken(
                @Nullable Long senderCurrentConsistencyToken
        ) {
            this.senderCurrentConsistencyToken = senderCurrentConsistencyToken;

            return this;
        }

        public TxStateResolutionTxStateResolutionParametersBuilder rowIdAndNewestCommitTimestamp(
                RowId rowId,
                @Nullable HybridTimestamp newestCommitTimestamp
        ) {
            this.rowId = rowId;
            this.newestCommitTimestamp = newestCommitTimestamp;

            return this;
        }

        public TxStateResolutionTxStateResolutionParametersBuilder awaitCommitPartitionAvailabilityTimeout(
                long awaitCommitPartitionAvailabilityTimeout,
                TimeUnit awaitCommitPartitionAvailabilityTimeUnit
        ) {
            this.awaitCommitPartitionAvailabilityTimeout = awaitCommitPartitionAvailabilityTimeout;
            this.awaitCommitPartitionAvailabilityTimeUnit = awaitCommitPartitionAvailabilityTimeUnit;

            return this;
        }

        public TxStateResolutionParameters build() {
            requireNonNull(txId, "txId cannot be null");
            requireNonNull(commitGroupId, "commitGroupId cannot be null");

            return new TxStateResolutionParameters(
                    txId,
                    commitGroupId,
                    readTimestamp,
                    senderCurrentConsistencyToken,
                    senderGroupId,
                    newestCommitTimestamp,
                    rowId,
                    awaitCommitPartitionAvailabilityTimeout,
                    awaitCommitPartitionAvailabilityTimeUnit
            );
        }
    }
}
