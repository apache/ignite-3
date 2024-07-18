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

package org.apache.ignite.internal.table.distributed.replicator;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STALE_OPERATION_ERR;

import java.util.UUID;
import org.apache.ignite.internal.tx.TransactionInternalException;

/** Error that occurs when a stale operation of a completed transaction is detected. */
// TODO: IGNITE-20415 - make this exception public.
public class StaleTransactionOperationException extends TransactionInternalException {
    /**
     * Constructor.
     *
     * @param txId Transaction ID.
     */
    public StaleTransactionOperationException(UUID txId) {
        super(TX_STALE_OPERATION_ERR, format("Stale operation of a completed transaction was detected: [txId={}]", txId));
    }
}
