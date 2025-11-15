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

package org.apache.ignite.internal.util;

/**
 * Busy lock implementation based on {@link IgniteStripedReadWriteLock}. API is analogous to {@link IgniteSpinBusyLock}, but without the
 * ability to unblock it.
 */
public class IgniteStripedBusyLock implements IgniteBusyLock {
    /** Underlying read-write lock. */
    private final IgniteStripedReadWriteLock lock = new IgniteStripedReadWriteLock();

    private volatile boolean blocked = false;

    /**
     * Enters "busy" state.
     *
     * @return {@code true} if entered to busy state.
     */
    @Override
    public boolean enterBusy() {
        if (!lock.readLock().tryLock()) {
            return false;
        }

        if (blocked) {
            leaveBusy();

            return false;
        }

        return true;
    }

    /**
     * Leaves "busy" state.
     */
    @Override
    public void leaveBusy() {
        lock.readLock().unlock();
    }

    /**
     * Blocks current thread till all activities left "busy" state and prevents them from further entering the "busy" state.
     */
    public void block() {
        lock.writeLock().lock();

        try {
            blocked = true;
        } finally {
            // Release the write lock and prevent its potential leak.
            lock.writeLock().unlock();
        }
    }
}
