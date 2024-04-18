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

package org.apache.ignite.internal.tx.impl;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxPriority;

/**
 * Generates transaction IDs.
 */
public class TransactionIdGenerator {
    /** Supplies nodeId for transactionId generation. */
    private final NodeIdSupplier nodeIdSupplier;

    public TransactionIdGenerator(NodeIdSupplier nodeIdSupplier) {
        this.nodeIdSupplier = nodeIdSupplier;
    }

    public TransactionIdGenerator(int nodeId) {
        this(() -> nodeId);
    }

    /**
     * Creates a transaction ID with the given begin timestamp.
     *
     * @param beginTimestamp Transaction begin timestamp.
     * @return Transaction ID.
     */
    public UUID transactionIdFor(HybridTimestamp beginTimestamp) {
        return transactionIdFor(beginTimestamp, TxPriority.NORMAL);
    }

    /**
     * Creates a transaction ID with the given begin timestamp.
     *
     * @param beginTimestamp Transaction begin timestamp.
     * @param priority Transaction priority.
     * @return Transaction ID.
     */
    public UUID transactionIdFor(HybridTimestamp beginTimestamp, TxPriority priority) {
        return TransactionIds.transactionId(beginTimestamp, nodeIdSupplier.nodeId(), priority);
    }
}
