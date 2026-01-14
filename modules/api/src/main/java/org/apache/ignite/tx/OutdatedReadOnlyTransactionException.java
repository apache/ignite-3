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

package org.apache.ignite.tx;

import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_OUTDATED_RO_TXN_ERR;

import java.util.UUID;

/**
 * Exception thrown when read-only transaction appears to be outdated due to its read timestamp that is less than low watermark.
 */
public class OutdatedReadOnlyTransactionException extends TransactionException {
    private static final long serialVersionUID = -7192328914800703670L;

    /**
     * Constructor.
     *
     * @param txId Transaction id.
     */
    public OutdatedReadOnlyTransactionException(UUID txId) {
        super(TX_OUTDATED_RO_TXN_ERR, "Outdated read-only transaction [txId" + txId + "].");
    }
}
