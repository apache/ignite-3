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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

class ReusableLockLockupTest {
    private final ReentrantLock lock = new ReentrantLock();

    @SuppressWarnings("resource")
    @Test
    void doesNotLockTheLockOnCreation() {
        ReusableLockLockup ignored = new ReusableLockLockup(lock);

        assertFalse(lock.isLocked());
    }

    @SuppressWarnings("resource")
    @Test
    void locksTheLockOnAcquire() {
        try (AutoLockup ignored = new ReusableLockLockup(lock).acquireLock()) {
            assertTrue(lock.isLocked());
        }
    }

    @SuppressWarnings({"resource", "EmptyTryBlock"})
    @Test
    void unlocksTheLockOnClose() {
        try (AutoLockup ignored = new ReusableLockLockup(lock).acquireLock()) {
            // No-op.
        }

        assertFalse(lock.isLocked());
    }
}