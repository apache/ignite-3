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

package org.apache.ignite.internal.storage.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.storage.RowId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LocalLocker}.
 */
public class LocalLockerTest {
    private LockByRowId lockByRowId;

    @BeforeEach
    void setUp() {
        lockByRowId = new LockByRowId();
    }

    @Test
    void testShouldReleaseReturnsFalseWhenNoSupplierProvided() {
        // Create locker without a supplier (default constructor)
        LocalLocker locker = new LocalLocker(lockByRowId);

        // shouldRelease should always return false
        assertFalse(locker.shouldRelease());
    }

    @Test
    void testShouldReleaseDynamicallyChangesWithSupplier() {
        // Create locker with a dynamic supplier
        AtomicBoolean shouldRelease = new AtomicBoolean(false);
        LocalLocker locker = new LocalLocker(lockByRowId, shouldRelease::get);

        // Initially should return false
        assertFalse(locker.shouldRelease());

        // Change the supplier's return value
        shouldRelease.set(true);
        assertTrue(locker.shouldRelease());

        // Change it back
        shouldRelease.set(false);
        assertFalse(locker.shouldRelease());
    }

    @Test
    void testShouldReleaseDoesNotAffectLocking() {
        // Create locker with a supplier that returns true
        LocalLocker locker = new LocalLocker(lockByRowId, () -> true);

        try {
            RowId rowId1 = new RowId(0, 1, 0);
            RowId rowId2 = new RowId(0, 2, 0);

            // shouldRelease returns true, but locking should still work normally
            assertTrue(locker.shouldRelease());

            locker.lock(rowId1);
            assertTrue(locker.isLocked(rowId1));
            assertFalse(locker.isLocked(rowId2));

            locker.lock(rowId2);
            assertTrue(locker.isLocked(rowId1));
            assertTrue(locker.isLocked(rowId2));

            // shouldRelease should still return true
            assertTrue(locker.shouldRelease());
        } finally {
            locker.unlockAll();
        }
    }

    @Test
    void testNullSupplierEquivalentToNoSupplier() {
        // Explicitly pass null as supplier
        LocalLocker lockerWithNull = new LocalLocker(lockByRowId, null);

        // Should behave the same way as created with no supplier in constructor
        assertFalse(lockerWithNull.shouldRelease());
    }
}
