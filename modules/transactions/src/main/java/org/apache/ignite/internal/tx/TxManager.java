/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.tx.impl.TransactionImpl;
import org.apache.ignite.lang.ByteArray;

/**
 * TODO: local tx ?
 */
public interface TxManager extends IgniteComponent {
    /**
     * Starts a transaction coordinated by local node.
     *
     * @return The transaction.
     */
    InternalTransaction begin();

    TxState state(Timestamp ts);

    boolean changeState(Timestamp ts, TxState before, TxState after);

    void forget(Timestamp ts);

    // TODO asch do we need async here ?
    CompletableFuture<Void> commitAsync(InternalTransaction transaction);

    CompletableFuture<Void> rollbackAsync(InternalTransaction transaction);

    /**
     * @param key The key.
     * @param tx The transaction.
     * @return The future.
     * @throws LockException When a lock can't be taken due to possible deadlock.
     */
    public CompletableFuture<Void> writeLock(ByteArray key, InternalTransaction tx);

    /**
     * @param key The key.
     * @param tx The transaction.
     * @return The future.
     * @throws LockException When a lock can't be taken due to possible deadlock.
     */
    public CompletableFuture<Void> readLock(ByteArray key, InternalTransaction tx);
}
