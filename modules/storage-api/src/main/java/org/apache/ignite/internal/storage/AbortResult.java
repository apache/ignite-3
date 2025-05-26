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
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Result of {@link MvPartitionStorage#abortWrite abort} of write intent. */
public class AbortResult {
    private static final AbortResult NO_WRITE_INTENT_ABORT_RESULT = new AbortResult(AbortResultStatus.NO_WRITE_INTENT, null, null);

    private final AbortResultStatus status;

    private final @Nullable UUID expectedTxId;

    private final @Nullable BinaryRow previousWriteIntent;

    /** Constructor. */
    private AbortResult(AbortResultStatus status, @Nullable UUID expectedTxId, @Nullable BinaryRow previousWriteIntent) {
        this.status = status;
        this.expectedTxId = expectedTxId;
        this.previousWriteIntent = previousWriteIntent;
    }

    /** Returns result of a successful abort of the write intent. */
    public static AbortResult success(@Nullable BinaryRow previousUncommittedRowVersion) {
        return new AbortResult(AbortResultStatus.SUCCESS, null, previousUncommittedRowVersion);
    }

    /** Returns the result if there is no write intent when attempting to abort it. */
    public static AbortResult noWriteIntent() {
        return NO_WRITE_INTENT_ABORT_RESULT;
    }

    /**
     * Returns the result of a failed attempt to abort a write intent by a transaction that did not add it.
     *
     * @see #expectedTxId()
     */
    public static AbortResult txMismatch(UUID expectedTxId) {
        return new AbortResult(AbortResultStatus.TX_MISMATCH, expectedTxId, null);
    }

    /** Returns the abort status of a write intent. */
    public AbortResultStatus status() {
        return status;
    }

    /**
     * Returns the transaction ID expected to abort the write intent. Transaction that added the write intent is expected to abort it. Not
     * {@code null} only for {@link AbortResultStatus#TX_MISMATCH}.
     */
    public @Nullable UUID expectedTxId() {
        return expectedTxId;
    }

    /**
     * Returns previous uncommitted write intent. Not {@code null} for {@link AbortResultStatus#SUCCESS} and if the previous write intent is
     * not a tombstone.
     */
    public @Nullable BinaryRow previousWriteIntent() {
        return previousWriteIntent;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
