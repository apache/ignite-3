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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RefCountedObjectPoolTest {

    @Spy
    private final TestValueProvider<Integer> valueProvider = new TestValueProvider<>();

    private RefCountedObjectPool<Long, Integer> pool;

    @BeforeEach
    void setUp() {
        pool = new RefCountedObjectPool<>();
    }

    @Test
    public void acquireAndRelease() {
        long key = 1L;

        Integer toBeReturned = 100;
        valueProvider.nextValue(toBeReturned);

        Integer value = pool.acquire(key, valueProvider);
        assertSame(toBeReturned, value);
        assertTrue(pool.isAcquired(key));

        assertTrue(pool.release(key));
        assertFalse(pool.isAcquired(key));

        verify(valueProvider, times(1)).get();
    }

    @Test
    public void valueCached() {
        long key = 1L;
        valueProvider.nextValue(100);

        Integer val1 = pool.acquire(key, valueProvider);
        Integer val2 = pool.acquire(key, valueProvider);

        assertSame(val1, val2);
        verify(valueProvider, times(1)).get();
    }

    @Test
    public void valueRemovedFromCacheAfterRelease() {
        long key = 1L;
        valueProvider.nextValue(100);

        Integer val1 = pool.acquire(key, valueProvider);

        assertTrue(pool.release(key));

        valueProvider.nextValue(200);
        Integer val2 = pool.acquire(key, valueProvider);

        assertNotSame(val1, val2);
        verify(valueProvider, times(2)).get();
    }

    @Test
    public void acquiredTwiceAndReleaseOnce() {
        long key = 1L;
        valueProvider.nextValue(100);
        Integer val1 = pool.acquire(key, valueProvider);

        valueProvider.nextValue(200);
        Integer val2 = pool.acquire(key, valueProvider);
        assertSame(val1, val2);
        assertFalse(pool.release(key));;

        assertTrue(pool.isAcquired(key));

        Integer val3 = pool.acquire(key, valueProvider);
        assertSame(val1, val3);

        assertFalse(pool.release(key));
        assertTrue(pool.release(key));

        assertFalse(pool.isAcquired(key));

        verify(valueProvider, times(1)).get();
    }

    private static class TestValueProvider<T> implements Supplier<T> {
        private T nextValue;

        private void nextValue(T nextValue) {
            this.nextValue = nextValue;
        }

        @Override
        public T get() {
            return nextValue;
        }
    }
}
