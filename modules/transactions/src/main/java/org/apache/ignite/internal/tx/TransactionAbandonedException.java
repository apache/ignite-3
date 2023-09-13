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

import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ABANDONED_ERR;

import java.util.UUID;
import org.apache.ignite.tx.TransactionException;

/**
 * Exception that is thrown when write intent can't be resolved because it has been created by a transaction that is now abandoned
 * (has a state that currently can't be known).
 */
public class TransactionAbandonedException extends TransactionException {
    private final UUID abandonedTxId;

    /**
     * Constructor.
     *
     * @param abandonedTxId ID of the transaction that is abandoned.
     */
    public TransactionAbandonedException(UUID abandonedTxId) {
        super(UUID.randomUUID(), TX_ABANDONED_ERR, null);

        this.abandonedTxId = abandonedTxId;
    }

    @Override
    public String getMessage() {
        return "Operation failed due to abandoned transaction [abandonedTxId=" + abandonedTxId + ']';
    }
}
