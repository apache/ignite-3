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

package org.apache.ignite.internal.table.distributed.raft;

import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_UNEXPECTED_STATE_ERR;

import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.tx.TransactionResult;

/**
 * The exception is thrown when a transaction state has already been set.
 */
public class UnexpectedTransactionStateException extends IgniteInternalException {
    private static final long serialVersionUID = 3791709101302280451L;

    /** Stored transaction result. */
    private final TransactionResult transactionResult;

    public UnexpectedTransactionStateException(String errorMessage, TransactionResult transactionResult) {
        super(TX_UNEXPECTED_STATE_ERR, errorMessage);
        this.transactionResult = transactionResult;
    }

    public TransactionResult transactionResult() {
        return transactionResult;
    }
}
