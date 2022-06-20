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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A transaction manager.
 */
public interface TxManager extends IgniteComponent {
    /**
     * Starts a transaction coordinated by a local node.
     *
     * @return The transaction.
     */
    InternalTransaction begin();

    /**
     * Returns a transaction state.
     *
     * @param txId Transaction id.
     * @return The state or null if the state is unknown.
     */
    @Nullable TxState state(UUID txId);

    /**
     * Atomically changes the state of a transaction.
     *
     * @param txId Transaction id.
     * @param before Before state.
     * @param after After state.
     * @return {@code True} if a state was changed.
     */
    boolean changeState(UUID txId, @Nullable TxState before, TxState after);

    /**
     * Forgets the transaction state. Intended for cleanup.
     *
     * @param txId Transaction id.
     */
    void forget(UUID txId);

    /**
     * Commits a transaction.
     *
     * @param txId Transaction id.
     * @return The future.
     */
    CompletableFuture<Void> commitAsync(UUID txId);

    /**
     * Aborts a transaction.
     *
     * @param txId Transaction id.
     * @return The future.
     */
    CompletableFuture<Void> rollbackAsync(UUID txId);

    /**
     * Acqures a write lock.
     *
     * @param lockId Table ID.
     * @param keyData The key data.
     * @param txId Transaction id.
     * @return The future.
     * @throws LockException When a lock can't be taken due to possible deadlock.
     */
    public CompletableFuture<Void> writeLock(IgniteUuid lockId, ByteBuffer keyData, UUID txId);

    /**
     * Acqures a read lock.
     *
     * @param lockId Lock id.
     * @param keyData The key data.
     * @param txId Transaction id.
     * @return The future.
     * @throws LockException When a lock can't be taken due to possible deadlock.
     */
    public CompletableFuture<Void> readLock(IgniteUuid lockId, ByteBuffer keyData, UUID txId);

    /**
     * Returns a transaction state or starts a new in the PENDING state.
     *
     * @param txId Transaction id.
     * @return @{code null} if a transaction was created, or a current state.
     */
    @Nullable
    TxState getOrCreateTransaction(UUID txId);

    /**
     * Finishes a dependant remote transactions.
     *
     * @param addr   The address.
     * @param commit {@code True} if a commit requested.
     * @param groups Enlisted partition groups.
     * @param txId   Transaction id.
     */
    CompletableFuture<Void> finishRemote(NetworkAddress addr, boolean commit, Set<String> groups, UUID txId);

    /**
     * Keys that are locked by the transaction.
     *
     * @param txId Transaction id.
     * @return Keys that are locked by the transaction.
     */
    @TestOnly
    Map<IgniteUuid, List<byte[]>> lockedKeys(UUID txId);

    /**
     * Checks if a passed address belongs to a local node.
     *
     * @param addr The address.
     * @return {@code True} if a local node.
     */
    boolean isLocal(NetworkAddress addr);

    /**
     * Returns a number of finished transactions.
     *
     * @return A number of finished transactions.
     */
    @TestOnly
    int finished();
}
