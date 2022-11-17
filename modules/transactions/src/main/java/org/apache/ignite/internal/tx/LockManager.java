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
import org.jetbrains.annotations.TestOnly;

/** Lock manager allows to acquire locks and release locks and supports deadlock prevention by transaction id ordering. */
public interface LockManager {

    /**
     * Attempts to acquire a lock for the specified {@code lockKey} in specified {@code lockMode}.
     *
     * @param txId Transaction id.
     * @param lockKey The key.
     * @param lockMode Lock mode, for example shared, exclusive, intention-shared etc.
     * @return The future with gained lock that will be completed when a lock is successfully acquired.
     * @throws LockException When a lock can't be taken due to possible deadlock.
     */
    public CompletableFuture<Lock> acquire(UUID txId, LockKey lockKey, LockMode lockMode);

    /**
     * Attempts to release a lock for the specified {@code lockKey}.
     *
     * @param lock Lock to release.
     */
    public void release(Lock lock);

    /**
     * Release a lock that holds on a specific mode.
     *
     * @param txId Transaction id.
     * @param lockKey The key.
     * @param lockMode Lock mode, for example shared, exclusive, intention-shared etc.
     */
    void release(UUID txId, LockKey lockKey, LockMode lockMode);

    /**
     * Retrieves all locks for the specified transaction id.
     *
     * @param txId Transaction Id.
     * @return An iterator over a collection of locks.
     */
    public Iterator<Lock> locks(UUID txId);

    /**
     * Returns a collection of transaction ids that is associated with the specified {@code key}.
     *
     * @param key The key.
     * @return The waiters queue.
     */
    @TestOnly
    public Collection<UUID> queue(LockKey key);

    /**
     * Returns a waiter associated with the specified {@code key}.
     *
     * @param key The key.
     * @param txId Transaction id.
     * @return The waiter.
     */
    @TestOnly
    public Waiter waiter(LockKey key, UUID txId);

    /**
     * Returns {@code true} if no locks have been held.
     *
     * @return {@code true} if no locks have been held.
     */
    @TestOnly
    public boolean isEmpty();
}
