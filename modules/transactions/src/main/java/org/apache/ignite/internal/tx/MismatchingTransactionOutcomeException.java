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

import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_UNEXPECTED_STATE_ERR;

/**
 * The exception is thrown when the transaction result differs from the intended one.
 *
 * <p>For example, {@code tx.commit()} is called for a transaction, but the verification logic decided to abort it instead. The transaction
 * will be finished with {@link TxState#ABORTED} and the call to {@code tx.commit()} will throw this exception.
 */
public class MismatchingTransactionOutcomeException extends TransactionInternalException {

    private static final long serialVersionUID = -7953057695915339651L;

    /** Stored transaction result. */
    private final TransactionResult transactionResult;

    /**
     * Constructor.
     */
    public MismatchingTransactionOutcomeException(int errorCode, String message, TransactionResult transactionResult, Throwable cause) {
        super(errorCode, message, cause);

        this.transactionResult = transactionResult;
    }

    public MismatchingTransactionOutcomeException(String message, TransactionResult transactionResult) {
        this(TX_UNEXPECTED_STATE_ERR, message, transactionResult, null);
    }

    public TransactionResult transactionResult() {
        return transactionResult;
    }
}
