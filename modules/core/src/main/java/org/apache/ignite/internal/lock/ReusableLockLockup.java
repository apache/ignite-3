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

package org.apache.ignite.internal.lock;

import java.util.concurrent.locks.Lock;

/**
 * {@link AutoLockup} around a {@link Lock} that is reused across lock acquire events. This means that each acquire
 * returns this instance, so the callers (that acquire and then own the underlying lock for some time) invoke {@link AutoLockup#close()}.
 * on same instance.
 *
 * <p>To acquire the lock, {@link #acquireLock()} needs to be called.
 */
public class ReusableLockLockup implements AutoLockup {
    private final Lock lock;

    private ReusableLockLockup(Lock lock) {
        this.lock = lock;
    }

    public static ReusableLockLockup forLock(Lock lock) {
        return new ReusableLockLockup(lock);
    }

    /**
     * Acquires the lock and returns this lockup.
     *
     * @return The lockup for the locked underlying lock.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    public AutoLockup acquireLock() {
        lock.lock();

        return this;
    }

    @Override
    public void close() {
        lock.unlock();
    }
}
