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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_WITH_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR;

/**
 * Common transaction error messages.
 */
public final class TransactionErrors {
    /** Transaction is already finished. */
    public static final String MESSAGE_TX_ALREADY_FINISHED = "Transaction is already finished";

    /** Transaction is already finished due to an error. */
    public static final String MESSAGE_TX_ALREADY_FINISHED_DUE_TO_ERR = "Transaction is already finished due to an error";

    /** Transaction is already finished due to timeout. */
    public static final String MESSAGE_TX_ALREADY_FINISHED_DUE_TO_TIMEOUT = "Transaction is already finished due to timeout";

    /**
     * Returns an error code for the "transaction already finished" family.
     *
     * @param isFinishedDueToTimeout Whether the transaction was finished due to timeout.
     * @param isFinishedDueToError Whether the transaction was finished due to an error.
     */
    public static int finishedTransactionErrorCode(boolean isFinishedDueToTimeout, boolean isFinishedDueToError) {
        if (isFinishedDueToTimeout) {
            return TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR;
        }

        return isFinishedDueToError ? TX_ALREADY_FINISHED_WITH_ERR : TX_ALREADY_FINISHED_ERR;
    }

    /**
     * Returns an error message for the "transaction already finished" family.
     *
     * @param isFinishedDueToTimeout Whether the transaction was finished due to timeout.
     * @param isFinishedDueToError Whether the transaction was finished due to an error.
     */
    public static String finishedTransactionErrorMessage(boolean isFinishedDueToTimeout, boolean isFinishedDueToError) {
        if (isFinishedDueToTimeout) {
            return MESSAGE_TX_ALREADY_FINISHED_DUE_TO_TIMEOUT;
        }

        return isFinishedDueToError ? MESSAGE_TX_ALREADY_FINISHED_DUE_TO_ERR : MESSAGE_TX_ALREADY_FINISHED;
    }

    private TransactionErrors() {
        // No-op.
    }
}
