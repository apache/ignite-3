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
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Result of {@link MvPartitionStorage#commitWrite commit} of write intent. */
public class CommitResult {
    private static final CommitResult SUCCESS_COMMIT_RESULT = new CommitResult(CommitResultStatus.SUCCESS, null);

    private static final CommitResult NO_WRITE_INTENT_COMMIT_RESULT = new CommitResult(CommitResultStatus.NO_WRITE_INTENT, null);

    private final CommitResultStatus status;

    private final @Nullable UUID expectedTxId;

    /** Constructor. */
    private CommitResult(CommitResultStatus status, @Nullable UUID expectedTxId) {
        this.status = status;
        this.expectedTxId = expectedTxId;
    }

    /** Returns result of a successful commit of the write intent. */
    public static CommitResult success() {
        return SUCCESS_COMMIT_RESULT;
    }

    /** Returns the result if there is no write intent when attempting to commit it. */
    public static CommitResult noWriteIntent() {
        return NO_WRITE_INTENT_COMMIT_RESULT;
    }

    /**
     * Returns the result of a failed attempt to commit a write intent by a transaction that did not add it.
     *
     * @see #expectedTxId()
     */
    public static CommitResult txMismatch(UUID expectedTxId) {
        return new CommitResult(CommitResultStatus.TX_MISMATCH, expectedTxId);
    }

    /** Returns the commit status of a write intent. */
    public CommitResultStatus status() {
        return status;
    }

    /**
     * Returns the transaction ID expected to commit the write intent. Transaction that added the write intent is expected to commit it. Not
     * {@code null} only for {@link CommitResultStatus#TX_MISMATCH}.
     */
    public @Nullable UUID expectedTxId() {
        return expectedTxId;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
