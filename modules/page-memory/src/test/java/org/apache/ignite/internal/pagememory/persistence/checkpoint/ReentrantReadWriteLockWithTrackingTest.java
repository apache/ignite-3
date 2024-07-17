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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * For {@link ReentrantReadWriteLockWithTracking} testing.
 */
public class ReentrantReadWriteLockWithTrackingTest extends BaseIgniteAbstractTest {
    @Test
    void testIsWriteLockedByCurrentThread() throws Exception {
        ReentrantReadWriteLockWithTracking lock0 = new ReentrantReadWriteLockWithTracking();
        ReentrantReadWriteLockWithTracking lock1 = new ReentrantReadWriteLockWithTracking(log, Long.MAX_VALUE);

        assertFalse(lock0.isWriteLockedByCurrentThread());
        assertFalse(lock1.isWriteLockedByCurrentThread());

        lock0.writeLock().lock();
        lock1.writeLock().lock();

        assertTrue(lock0.isWriteLockedByCurrentThread());
        assertTrue(lock1.isWriteLockedByCurrentThread());

        runAsync(() -> {
            assertFalse(lock0.isWriteLockedByCurrentThread());
            assertFalse(lock1.isWriteLockedByCurrentThread());
        }).get(1, TimeUnit.SECONDS);

        lock0.writeLock().unlock();
        lock1.writeLock().unlock();

        assertFalse(lock0.isWriteLockedByCurrentThread());
        assertFalse(lock1.isWriteLockedByCurrentThread());
    }

    @Test
    void testGetReadHoldCount() throws Exception {
        ReentrantReadWriteLockWithTracking lock0 = new ReentrantReadWriteLockWithTracking();
        ReentrantReadWriteLockWithTracking lock1 = new ReentrantReadWriteLockWithTracking(log, Long.MAX_VALUE);

        assertEquals(0, lock0.getReadHoldCount());
        assertEquals(0, lock1.getReadHoldCount());

        lock0.readLock().lock();
        lock0.readLock().lock();

        lock1.readLock().lock();

        assertEquals(2, lock0.getReadHoldCount());
        assertEquals(1, lock1.getReadHoldCount());

        runAsync(() -> {
            assertEquals(0, lock0.getReadHoldCount());
            assertEquals(0, lock1.getReadHoldCount());

            lock0.readLock().lock();
            lock1.readLock().lock();

            assertEquals(1, lock0.getReadHoldCount());
            assertEquals(1, lock1.getReadHoldCount());

            lock0.readLock().unlock();
            lock1.readLock().unlock();

            assertEquals(0, lock0.getReadHoldCount());
            assertEquals(0, lock1.getReadHoldCount());
        }).get(1, TimeUnit.SECONDS);

        assertEquals(2, lock0.getReadHoldCount());
        assertEquals(1, lock1.getReadHoldCount());

        lock0.readLock().unlock();
        lock1.readLock().unlock();

        assertEquals(1, lock0.getReadHoldCount());
        assertEquals(0, lock1.getReadHoldCount());

        lock0.readLock().unlock();

        assertEquals(0, lock0.getReadHoldCount());
        assertEquals(0, lock1.getReadHoldCount());
    }

    @Test
    void testGetReadLockCount() throws Exception {
        ReentrantReadWriteLockWithTracking lock0 = new ReentrantReadWriteLockWithTracking();
        ReentrantReadWriteLockWithTracking lock1 = new ReentrantReadWriteLockWithTracking(log, Long.MAX_VALUE);

        assertEquals(0, lock0.getReadLockCount());
        assertEquals(0, lock1.getReadLockCount());

        lock0.readLock().lock();
        lock0.readLock().lock();

        lock1.readLock().lock();

        assertEquals(2, lock0.getReadLockCount());
        assertEquals(1, lock1.getReadLockCount());

        runAsync(() -> {
            assertEquals(2, lock0.getReadLockCount());
            assertEquals(1, lock1.getReadLockCount());

            lock0.readLock().lock();
            lock1.readLock().lock();

            assertEquals(3, lock0.getReadLockCount());
            assertEquals(2, lock1.getReadLockCount());

            lock0.readLock().unlock();
            lock1.readLock().unlock();

            assertEquals(2, lock0.getReadLockCount());
            assertEquals(1, lock1.getReadLockCount());
        }).get(1, TimeUnit.SECONDS);

        assertEquals(2, lock0.getReadLockCount());
        assertEquals(1, lock1.getReadLockCount());

        lock0.readLock().unlock();
        lock1.readLock().unlock();

        assertEquals(1, lock0.getReadLockCount());
        assertEquals(0, lock1.getReadLockCount());

        lock0.readLock().unlock();

        assertEquals(0, lock0.getReadLockCount());
        assertEquals(0, lock1.getReadLockCount());
    }

    @Test
    void testPrintLongHoldReadLock() throws Exception {
        IgniteLogger log = mock(IgniteLogger.class);

        ArgumentCaptor<String> msgArgumentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Throwable> throwableArgumentCaptor = ArgumentCaptor.forClass(Throwable.class);
        ArgumentCaptor<Object[]> objectArrayArgumentCaptor = ArgumentCaptor.forClass(Object[].class);

        doNothing().when(log).warn(msgArgumentCaptor.capture(), throwableArgumentCaptor.capture(), objectArrayArgumentCaptor.capture());

        ReentrantReadWriteLockWithTracking lock0 = new ReentrantReadWriteLockWithTracking(log, 20);
        ReentrantReadWriteLockWithTracking lock1 = new ReentrantReadWriteLockWithTracking(log, 200);

        lock0.readLock().lock();
        lock1.readLock().lock();

        Thread.sleep(50);

        lock0.readLock().unlock();
        lock1.readLock().unlock();

        assertThat(msgArgumentCaptor.getAllValues(), hasSize(1));
        assertThat(throwableArgumentCaptor.getAllValues(), hasSize(1));

        assertThat(msgArgumentCaptor.getValue(), Matchers.startsWith("ReadLock held for too long"));
        assertThat(throwableArgumentCaptor.getValue(), instanceOf(IgniteInternalException.class));
    }
}
