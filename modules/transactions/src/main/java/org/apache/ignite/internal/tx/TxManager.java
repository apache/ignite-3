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
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface TxManager extends IgniteComponent {
    /**
     * Starts a transaction coordinated by local node.
     *
     * @return The transaction.
     */
    InternalTransaction begin();

    /**
     * @param ts The timestamp.
     * @return The state or null if the state is unknown.
     */
    @Nullable TxState state(Timestamp ts);

    /**
     * @param ts The timestamp.
     * @param before Before state.
     * @param after After state.
     * @return {@code True} if a state was changed.
     */
    boolean changeState(Timestamp ts, TxState before, TxState after);

    /**
     * @param ts The timestamp.
     */
    void forget(Timestamp ts);

    /**
     * @param ts The timestamp.
     * @return The future.
     */
    CompletableFuture<Void> commitAsync(Timestamp ts);

    /**
     * @param ts The timestamp.
     * @return The future.
     */
    CompletableFuture<Void> rollbackAsync(Timestamp ts);

    /**
     * @param key The key.
     * @param ts The timestamp.
     * @return The future.
     * @throws LockException When a lock can't be taken due to possible deadlock.
     */
    public CompletableFuture<Void> writeLock(ByteArray key, Timestamp ts);

    /**
     * @param key The key.
     * @param ts The timestamp.
     * @return The future.
     * @throws LockException When a lock can't be taken due to possible deadlock.
     */
    public CompletableFuture<Void> readLock(ByteArray key, Timestamp ts);

    /**
     * @param timestamp The timestamp.
     * @return {@code True} if a transaction was created in PENDING state.
     */
    boolean getOrCreateTransaction(Timestamp ts);

    /**
     * @param addr The address.
     * @param timestamp The timestamp.
     * @param commit {@code True} if a commit requested.
     */
    CompletableFuture<TxState> sendFinishMessage(NetworkAddress addr, Timestamp ts, boolean commit);

    /**
     * @param addr The address.
     * @return {@code True} if a local node.
     */
    boolean isLocal(NetworkAddress addr);
}
