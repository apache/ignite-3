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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RefCountedObjectPoolTest {
    private RefCountedObjectPool<Long, Integer> pool;

    @BeforeEach
    void setUp() {
        pool = new RefCountedObjectPool<>();
    }

    @Test
    public void acquireAndRelease() {
        long key = 1L;
        Integer toBeReturned = 100;

        Integer value = pool.acquire(key, ignored -> toBeReturned);
        assertSame(toBeReturned, value);
        assertTrue(pool.isAcquired(key));

        assertTrue(pool.release(key));
        assertFalse(pool.isAcquired(key));
    }

    @Test
    public void valueCached() {
        long key = 1L;

        Integer val1 = pool.acquire(key, ignored -> 100);
        Integer val2 = pool.acquire(key, ignored -> 300);

        assertSame(val1, val2);
    }

    @Test
    public void valueRemovedFromCacheAfterRelease() {
        long key = 1L;

        Integer val1 = pool.acquire(key, ignored -> 100);

        assertTrue(pool.release(key));

        Integer val2 = pool.acquire(key, ignored -> 200);

        assertNotSame(val1, val2);
    }

    @Test
    public void acquiredTwiceAndReleaseOnce() {
        long key = 1L;
        Integer val1 = pool.acquire(key, ignored -> 100);

        Integer val2 = pool.acquire(key, ignored -> 200);
        assertSame(val1, val2);
        assertFalse(pool.release(key));

        assertTrue(pool.isAcquired(key));

        Integer val3 = pool.acquire(key, ignored -> 200);
        assertSame(val1, val3);

        assertFalse(pool.release(key));
        assertTrue(pool.release(key));

        assertFalse(pool.isAcquired(key));
    }
}
