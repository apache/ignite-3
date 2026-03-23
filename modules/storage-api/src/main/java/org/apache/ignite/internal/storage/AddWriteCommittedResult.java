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
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Result of {@link MvPartitionStorage#addWriteCommitted add} of write intent committed. */
public class AddWriteCommittedResult {
    private static final AddWriteCommittedResult SUCCESS_ADD_WRITE_COMMITTED_RESULT = new AddWriteCommittedResult(
            AddWriteCommittedResultStatus.SUCCESS, null, null
    );

    private final AddWriteCommittedResultStatus status;

    private final @Nullable UUID currentWriteIntentTxId;

    private final @Nullable HybridTimestamp latestCommitTimestamp;

    /** Constructor. */
    private AddWriteCommittedResult(
            AddWriteCommittedResultStatus status,
            @Nullable UUID currentWriteIntentTxId,
            @Nullable HybridTimestamp latestCommitTimestamp
    ) {
        this.status = status;
        this.currentWriteIntentTxId = currentWriteIntentTxId;
        this.latestCommitTimestamp = latestCommitTimestamp;
    }

    /** Returns result of a successful add of the write intent committed. */
    public static AddWriteCommittedResult success() {
        return SUCCESS_ADD_WRITE_COMMITTED_RESULT;
    }

    /** Returns result when an uncommitted write intent was found while adding a new one committed. */
    public static AddWriteCommittedResult writeIntentExists(
            UUID currentWriteIntentTxId,
            @Nullable HybridTimestamp latestCommitTimestamp
    ) {
        return new AddWriteCommittedResult(
                AddWriteCommittedResultStatus.WRITE_INTENT_EXISTS,
                currentWriteIntentTxId,
                latestCommitTimestamp
        );
    }

    /** Returns the add status of a write intent committed. */
    public AddWriteCommittedResultStatus status() {
        return status;
    }

    /**
     * Returns the transaction ID of the current write intent. Not {@code null} for
     * {@link AddWriteCommittedResultStatus#WRITE_INTENT_EXISTS}.
     */
    public @Nullable UUID currentWriteIntentTxId() {
        return currentWriteIntentTxId;
    }

    /**
     * Returns commit timestamp of latest committed version. Not {@code null} for
     * {@link AddWriteCommittedResultStatus#WRITE_INTENT_EXISTS} and if present.
     */
    public @Nullable HybridTimestamp latestCommitTimestamp() {
        return latestCommitTimestamp;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
