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

import static org.apache.ignite.internal.tx.LockMode.S;
import static org.apache.ignite.internal.tx.LockMode.X;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.apache.ignite.internal.tx.impl.WaiterImpl;
import org.junit.jupiter.api.Test;

public class WaiterTest {
    @Test
    public void acquireReleaseTest() {
        Waiter waiter = new WaiterImpl(UUID.randomUUID());

        assertFalse(waiter.locked());

        waiter.addLock(S, false);

        assertTrue(waiter.locked());

        waiter.addLock(X, true);

        assertEquals(S, waiter.lockMode());
    }


    /*@Test
    public void test() {
        Waiter waiter = new WaiterImpl(UUID.randomUUID());

        for (LockMode mode : LockMode.values()) {
            assertEquals(0, waiter.locked());
        }

        int minCount = 0x7AAA;

        int a = 0;
        for (LockMode mode : LockMode.values()) {
            a++;
            multipleAcquire(waiter, mode, a + minCount);
        }

        a = 0;
        for (LockMode mode : LockMode.values()) {
            a++;
            assertEquals(a + minCount, map.count(mode));
        }

        a = 0;
        for (LockMode mode : LockMode.values()) {
            a++;
            map.increment(mode);
            assertEquals(a + minCount + 1, map.count(mode));
            map.decrement(mode);
            assertEquals(a + minCount, map.count(mode));
        }

        a = 0;
        for (LockMode mode : LockMode.values()) {
            a++;
            multipleRelease(waiter, mode, a + minCount);
        }

        for (LockMode mode : LockMode.values()) {
            assertEquals(0, waiter.count(mode));
        }
    }

    private void multipleAcquire(Waiter waiter, LockMode lockMode, int count) {
        for (int i = 0; i < count; i++) {
            waiter.addLock(lockMode, true);
        }
    }

    private void multipleRelease(Waiter waiter, LockMode lockMode, int count) {
        for (int i = 0; i < count; i++) {
            waiter.addLock(lockMode, true);
        }
    }*/
}
