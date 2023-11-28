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

package org.apache.ignite.internal.table.distributed.schema;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tx.InternalTransaction;

/**
 * Logic around transaction-related timestamps.
 */
public interface TransactionTimestamps {
    /**
     * Returns <i>base timestamp</i> corresponding to the given transaction.
     *
     * <p>Base timestamp is the timestamp at which schemas are taken with which the transaction
     * works throughout its lifetime.
     *
     * @param tx Transaction for which to determine its base timestamp.
     * @param tableId ID of the table relative to which the timestamp is determined.
     * @return Future that will be completed with the timestamp.
     */
    CompletableFuture<HybridTimestamp> baseTimestamp(InternalTransaction tx, int tableId);

    /**
     * Returns <i>base timestamp</i> corresponding to an RW transaction started at the given timestamp.
     *
     * <p>Base timestamp is the timestamp at which schemas are taken with which the transaction
     * works throughout its lifetime.
     *
     * @param txBeginTimestamp Transaction begin timestamp.
     * @param tableId ID of the table relative to which the timestamp is determined.
     * @return Future that will be completed with the timestamp.
     */
    CompletableFuture<HybridTimestamp> rwTransactionBaseTimestamp(HybridTimestamp txBeginTimestamp, int tableId);
}
