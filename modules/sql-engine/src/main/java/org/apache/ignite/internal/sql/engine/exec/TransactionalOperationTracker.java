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

package org.apache.ignite.internal.sql.engine.exec;

import org.apache.ignite.internal.tx.InternalTransaction;

/**
 * Tracker of ongoing operations within a transaction.
 *
 * <p>Complex operations may involve interaction with multiple partitions on multiple nodes. This interface allows to
 * atomically mark boundaries of operation. Let's assume we need to scan several partitions. Before 
 * {@link #registerOperationStart(InternalTransaction)} is called no interactions with storage are expected
 * within the transaction (NB: concurrent operations may still access storage within the same transaction). Similar
 * with {@link #registerOperationFinish(InternalTransaction)}: it marks the point after which no interaction with 
 * storage are expected.
 */
public interface TransactionalOperationTracker {
    /**
     * Marks the beginning of operation.
     *
     * @param tx Related transaction within which operation is performed.
     */
    void registerOperationStart(InternalTransaction tx);

    /**
     * Marks the end of operation.
     *
     * @param tx Related transaction within which operation was performed.
     */
    void registerOperationFinish(InternalTransaction tx);
}
