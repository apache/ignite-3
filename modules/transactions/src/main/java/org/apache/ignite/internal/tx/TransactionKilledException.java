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

import static org.apache.ignite.internal.tx.TransactionLogUtils.formatTxInfo;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_KILLED_ERR;

import java.util.UUID;
import org.apache.ignite.tx.RetriableTransactionException;

/**
 * Reports killed transaction.
 */
public class TransactionKilledException extends TransactionInternalException implements RetriableTransactionException {
    private static final long serialVersionUID = 0L;

    private final UUID txId;

    /**
     * Creates the exception with txId and optional txManager for label formatting.
     *
     * @param txId The transaction id.
     * @param txManager transaction manager to retrieve label for logging.
     */
    public TransactionKilledException(UUID txId, TxManager txManager) {
        super(
                TX_KILLED_ERR,
                "Transaction is killed " + formatTxInfo(txId, txManager)
        );
        this.txId = txId;
    }

    /**
     * Returns a transaction id.
     *
     * @return The id.
     */
    public UUID txId() {
        return txId;
    }
}
