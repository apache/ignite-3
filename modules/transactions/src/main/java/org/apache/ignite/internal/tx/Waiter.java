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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * The lock waiter.
 */
public interface Waiter extends Comparable<Waiter> {
    /**
     * Associated transaction id.
     *
     * @return Associated transaction id.
     */
    UUID txId();

    /**
     * Returns lock state.
     *
     * @return {@code true} if a waiter holds the lock.
     */
    boolean locked();

    void lock();

    void fail(LockException e);

    CompletableFuture<Void> fut();

    void addLock(LockMode lockMode, boolean intention);

    void removeLock(LockMode lockMode);

    /**
     * Returns lock mode.
     *
     * @return Lock mode.
     */
    LockMode lockMode();

    LockMode intentionLockMode();
}
