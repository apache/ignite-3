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
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Result of {@link MvPartitionStorage#addWrite add} of write intent. */
public class AddWriteResult {
    private final AddWriteResultStatus status;

    private final @Nullable BinaryRow previousWriteIntent;

    private final @Nullable UUID currentWriteIntentTxId;

    private final @Nullable HybridTimestamp latestCommitTimestamp;

    /** Constructor. */
    private AddWriteResult(
            AddWriteResultStatus status,
            @Nullable BinaryRow previousWriteIntent,
            @Nullable UUID currentWriteIntentTxId,
            @Nullable HybridTimestamp latestCommitTimestamp
    ) {
        this.status = status;
        this.previousWriteIntent = previousWriteIntent;
        this.currentWriteIntentTxId = currentWriteIntentTxId;
        this.latestCommitTimestamp = latestCommitTimestamp;
    }

    /** Returns result of a successful add of the write intent or replace for same transaction. */
    public static AddWriteResult success(@Nullable BinaryRow previousWriteIntent) {
        return new AddWriteResult(AddWriteResultStatus.SUCCESS, previousWriteIntent, null, null);
    }

    /** Returns result when an uncommitted write intent of another transaction was found while adding a new one. */
    public static AddWriteResult txMismatch(UUID currentWriteIntentTxId, @Nullable HybridTimestamp latestCommitTimestamp) {
        return new AddWriteResult(AddWriteResultStatus.TX_MISMATCH, null, currentWriteIntentTxId, latestCommitTimestamp);
    }

    /** Returns the add status of a write intent. */
    public AddWriteResultStatus status() {
        return status;
    }

    /**
     * Returns the previous write intent. Not {@code null} for a {@link AddWriteResultStatus#SUCCESS successful} result and a write intent
     * replace occurred for the same transaction.
     */
    public @Nullable BinaryRow previousWriteIntent() {
        return previousWriteIntent;
    }

    /** Returns the transaction ID of the current write intent. Not {@code null} for {@link AddWriteResultStatus#TX_MISMATCH}. */
    public @Nullable UUID currentWriteIntentTxId() {
        return currentWriteIntentTxId;
    }

    /**
     * Returns commit timestamp of latest committed version. Not {@code null} for {@link AddWriteResultStatus#TX_MISMATCH} and if present.
     */
    public @Nullable HybridTimestamp latestCommitTimestamp() {
        return latestCommitTimestamp;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
