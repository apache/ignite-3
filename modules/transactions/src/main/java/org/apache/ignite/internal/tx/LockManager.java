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

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.event.EventProducer;
import org.apache.ignite.internal.tx.event.LockEvent;
import org.apache.ignite.internal.tx.event.LockEventParameters;
import org.jetbrains.annotations.TestOnly;

/** Lock manager allows to acquire locks and release locks and supports deadlock prevention by transaction id ordering. */
public interface LockManager extends EventProducer<LockEvent, LockEventParameters> {
    /**
     * Start the lock manager.
     *
     * @param deadlockPreventionPolicy Deadlock prevention policy.
     */
    void start(DeadlockPreventionPolicy deadlockPreventionPolicy);

    /**
     * Attempts to acquire a lock for the specified {@code lockKey} in specified {@code lockMode}.
     *
     * @param txId Transaction id.
     * @param lockKey The key.
     * @param lockMode Lock mode, for example shared, exclusive, intention-shared etc.
     * @return The future with gained lock that will be completed when a lock is successfully acquired.
     */
    CompletableFuture<Lock> acquire(UUID txId, LockKey lockKey, LockMode lockMode);

    /**
     * Attempts to release the specified lock.
     *
     * @param lock Lock to release.
     */
    @TestOnly
    void release(Lock lock);

    /**
     * Release a lock that is held on the specific mode on the specific key.
     *
     * @param txId Transaction id.
     * @param lockKey The key.
     * @param lockMode Lock mode, for example shared, exclusive, intention-shared etc.
     */
    void release(UUID txId, LockKey lockKey, LockMode lockMode);

    /**
     * Retrieves all locks.
     *
     * @return An iterator over a collection of locks.
     */
    Iterator<Lock> locks();

    /**
     * Retrieves all locks for the specified transaction id.
     *
     * @param txId Transaction Id.
     * @return An iterator over a collection of locks.
     */
    @TestOnly
    Iterator<Lock> locks(UUID txId);

    /**
     * Release all locks associated with a transaction.
     *
     * @param txId Tx id.
     */
    void releaseAll(UUID txId);

    /**
     * Fail all waiters with the cause.
     *
     * @param txId Tx id.
     * @param cause The cause.
     */
    void failAllWaiters(UUID txId, Exception cause);

    /**
     * Returns a collection of transaction ids that is associated with the specified {@code key}.
     *
     * @param key The key.
     * @return The waiters queue.
     */
    @TestOnly
    Collection<UUID> queue(LockKey key);

    /**
     * Returns a waiter associated with the specified {@code key}.
     *
     * @param key The key.
     * @param txId Transaction id.
     * @return The waiter.
     */
    @TestOnly
    Waiter waiter(LockKey key, UUID txId);

    /**
     * Returns {@code true} if no locks have been held.
     *
     * @return {@code true} if no locks have been held.
     */
    @TestOnly
    boolean isEmpty();
}
