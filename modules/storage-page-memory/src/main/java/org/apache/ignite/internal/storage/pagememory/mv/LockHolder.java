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

package org.apache.ignite.internal.storage.pagememory.mv;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

/**
 * Class to keep track of the number of lock holders({@link Thread}).
 *
 * <p>Honest holder tracking requires an external sync, such as a {@link ConcurrentMap}.
 *
 * <p>How to use:
 * <ul>
 *     <li>Increases the count of lock holders by {@link #incrementHolders()};</li>
 *     <li>You can {@link #getLock() get a lock} and work with it;</li>
 *     <li>After lock is no longer used, you need to call {@link #decrementHolders()}, which will tell you if lock holders are left.</li>
 * </ul>
 */
class LockHolder<T extends Lock> {
    private final T lock;

    private final AtomicInteger lockHolder = new AtomicInteger();

    private static final ThreadLocal<Thread> HOLDER = new ThreadLocal<>();

    LockHolder(T lock) {
        this.lock = lock;
    }

    /**
     * Increment the count of lock holders ({@link Thread}).
     */
    void incrementHolders() {
        if (HOLDER.get() == null) {
            HOLDER.set(Thread.currentThread());

            int holders = lockHolder.incrementAndGet();

            assert holders > 0 : holders;
        } else {
            assert HOLDER.get() == Thread.currentThread() : "holderThread=" + HOLDER.get() + ", currentThread=" + Thread.currentThread();

            int holders = lockHolder.get();

            assert holders > 0 : holders;
        }
    }

    /**
     * Decrements the count of lock holders ({@link Thread}), returns {@code true} if there are no more lock holders.
     */
    boolean decrementHolders() {
        assert HOLDER.get() == Thread.currentThread() : "holderThread=" + HOLDER.get() + ", currentThread=" + Thread.currentThread();

        HOLDER.set(null);

        int holders = lockHolder.decrementAndGet();

        assert holders >= 0 : holders;

        return holders == 0;
    }

    /**
     * Returns lock.
     */
    T getLock() {
        assert HOLDER.get() == Thread.currentThread() : "holderThread=" + HOLDER.get() + ", currentThread=" + Thread.currentThread();

        return lock;
    }
}
