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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests a LockManager implementation.
 */
public abstract class AbstractLockManagerTest extends IgniteAbstractTest {
    private LockManager lockManager;

    @BeforeEach
    public void before() {
        lockManager = newInstance();
    }

    protected abstract LockManager newInstance();

    @Test
    public void testSingleKeyWrite() throws LockException {
        UUID txId1 = Timestamp.nextVersion().toUuid();

        Object key = new String("test");

        CompletableFuture<Void> fut0 = lockManager.tryAcquire(key, txId1);

        assertTrue(fut0.isDone());

        Collection<UUID> queue = lockManager.queue(key);

        assertTrue(queue.size() == 1 && queue.iterator().next().equals(txId1));

        Waiter waiter = lockManager.waiter(key, txId1);

        assertTrue(waiter.locked());

        lockManager.tryRelease(key, txId1);
    }

    @Test
    public void testSingleKeyWriteLock() throws LockException {
        UUID txId1 = Timestamp.nextVersion().toUuid();

        Object key = new String("test");

        CompletableFuture<Void> fut0 = lockManager.tryAcquire(key, txId1);

        assertTrue(fut0.isDone());

        UUID txId2 = Timestamp.nextVersion().toUuid();

        assertTrue(txId1.compareTo(txId2) < 0);

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, txId2);

        assertFalse(fut1.isDone());

        assertTrue(lockManager.waiter(key, txId1).locked());
        assertFalse(lockManager.waiter(key, txId2).locked());

        lockManager.tryRelease(key, txId1);

        assertTrue(fut1.isDone());

        assertNull(lockManager.waiter(key, txId1));
        assertTrue(lockManager.waiter(key, txId2).locked());

        lockManager.tryRelease(key, txId2);

        assertNull(lockManager.waiter(key, txId1));
        assertNull(lockManager.waiter(key, txId2));
    }

    @Test
    public void testSingleKeyReadWriteLock() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();
        UUID txId3 = Timestamp.nextVersion().toUuid();
        assertTrue(txId0.compareTo(txId1) < 0);
        assertTrue(txId1.compareTo(txId2) < 0);
        assertTrue(txId2.compareTo(txId3) < 0);
        Object key = new String("test");

        CompletableFuture<Void> fut0 = lockManager.tryAcquireShared(key, txId0);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut2 = lockManager.tryAcquireShared(key, txId2);
        assertTrue(fut2.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquireShared(key, txId1);
        assertTrue(fut1.isDone());

        CompletableFuture<Void> fut3 = lockManager.tryAcquire(key, txId3);
        assertFalse(fut3.isDone());

        assertTrue(lockManager.waiter(key, txId0).locked());
        assertTrue(lockManager.waiter(key, txId1).locked());
        assertTrue(lockManager.waiter(key, txId2).locked());
        assertFalse(lockManager.waiter(key, txId3).locked());

        lockManager.tryReleaseShared(key, txId2);

        assertTrue(lockManager.waiter(key, txId0).locked());
        assertTrue(lockManager.waiter(key, txId1).locked());
        assertNull(lockManager.waiter(key, txId2));
        assertFalse(lockManager.waiter(key, txId3).locked());

        lockManager.tryReleaseShared(key, txId0);

        assertNull(lockManager.waiter(key, txId0));
        assertTrue(lockManager.waiter(key, txId1).locked());
        assertNull(lockManager.waiter(key, txId2));
        assertFalse(lockManager.waiter(key, txId3).locked());

        lockManager.tryReleaseShared(key, txId1);

        assertNull(lockManager.waiter(key, txId0));
        assertNull(lockManager.waiter(key, txId1));
        assertNull(lockManager.waiter(key, txId2));
        assertTrue(lockManager.waiter(key, txId3).locked());
    }

    @Test
    public void testSingleKeyReadWriteConflict() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        Object key = new String("test");

        // Lock in order
        CompletableFuture<Void> fut0 = lockManager.tryAcquireShared(key, txId0);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, txId1);
        assertFalse(fut1.isDone());

        lockManager.tryReleaseShared(key, txId0);
        assertTrue(fut1.isDone());

        lockManager.tryRelease(key, txId1);

        assertTrue(lockManager.queue(key).isEmpty());

        // Lock not in order
        fut0 = lockManager.tryAcquireShared(key, txId1);
        assertTrue(fut0.isDone());

        try {
            lockManager.tryAcquire(key, txId0).join();

            fail();
        } catch (CompletionException e) {
            // Expected.
        }
    }

    @Test
    public void testSingleKeyReadWriteConflict2() throws LockException {
        UUID[] txId = generate(3);
        Object key = new String("test");

        // Lock in order
        CompletableFuture<Void> fut0 = lockManager.tryAcquireShared(key, txId[1]);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, txId[2]);
        assertFalse(fut1.isDone());

        CompletableFuture<Void> fut2 = lockManager.tryAcquireShared(key, txId[0]);
        assertTrue(fut2.isDone());

        lockManager.tryReleaseShared(key, txId[1]);
        lockManager.tryReleaseShared(key, txId[0]);

        assertTrue(fut1.isDone());
    }

    @Test
    public void testSingleKeyReadWriteConflict3() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();
        Object key = new String("test");

        // Lock in order
        CompletableFuture<Void> fut0 = lockManager.tryAcquireShared(key, txId0);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, txId2);
        assertFalse(fut1.isDone());

        CompletableFuture<Void> fut2 = lockManager.tryAcquireShared(key, txId1);
        assertTrue(fut2.isDone());

        assertFalse(lockManager.waiter(key, txId2).locked());

        lockManager.tryReleaseShared(key, txId1);
        lockManager.tryReleaseShared(key, txId0);

        assertTrue(fut1.isDone());
    }

    @Test
    public void testSingleKeyReadWriteConflict4() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        final UUID txId2 = Timestamp.nextVersion().toUuid();
        UUID txId3 = Timestamp.nextVersion().toUuid();
        Object key = new String("test");

        CompletableFuture<Void> fut0 = lockManager.tryAcquireShared(key, txId0);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, txId1);
        assertFalse(fut1.isDone());

        CompletableFuture<Void> fut2 = lockManager.tryAcquire(key, txId3);
        assertFalse(fut2.isDone());

        CompletableFuture<Void> fut3 = lockManager.tryAcquire(key, txId2);
        assertFalse(fut3.isDone());
    }

    @Test
    public void testSingleKeyReadWriteConflict5() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        Object key = new String("test");

        lockManager.tryAcquire(key, txId1).join();

        expectConflict(lockManager.tryAcquire(key, txId0));
    }

    @Test
    public void testSingleKeyReadWriteConflict6() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        Object key = new String("test");

        lockManager.tryAcquireShared(key, txId0).join();

        lockManager.tryAcquireShared(key, txId1).join();

        CompletableFuture<Void> fut = lockManager.tryAcquire(key, txId1);
        assertFalse(fut.isDone());

        lockManager.tryAcquire(key, txId0).join();

        lockManager.tryRelease(key, txId0);

        expectConflict(fut);
    }

    @Test
    public void testSingleKeyWriteWriteConflict() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();
        Object key = new String("test");

        // Lock in order
        CompletableFuture<Void> fut0 = lockManager.tryAcquire(key, txId1);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, txId2);
        assertFalse(fut1.isDone());

        try {
            lockManager.tryAcquire(key, txId0).join();

            fail();
        } catch (CompletionException e) {
            // Expected.
        }
    }

    @Test
    public void testSingleKeyWriteWriteConflict2() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();
        Object key = new String("test");

        // Lock in order
        CompletableFuture<Void> fut0 = lockManager.tryAcquire(key, txId0);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, txId2);
        assertFalse(fut1.isDone());

        CompletableFuture<Void> fut2 = lockManager.tryAcquire(key, txId1);
        assertFalse(fut2.isDone());
    }

    @Test
    public void testSingleKeyMultithreadedRead() throws InterruptedException {
        LongAdder readLocks = new LongAdder();
        LongAdder writeLocks = new LongAdder();
        LongAdder failedLocks = new LongAdder();

        doTestSingleKeyMultithreaded(5_000, readLocks, writeLocks, failedLocks, 0);

        assertTrue(writeLocks.sum() == 0);
        assertTrue(failedLocks.sum() == 0);
    }

    @Test
    public void testSingleKeyMultithreadedWrite() throws InterruptedException {
        LongAdder readLocks = new LongAdder();
        LongAdder writeLocks = new LongAdder();
        LongAdder failedLocks = new LongAdder();

        doTestSingleKeyMultithreaded(5_000, readLocks, writeLocks, failedLocks, 1);

        assertTrue(readLocks.sum() == 0);
    }

    @Test
    public void testSingleKeyMultithreadedRandom() throws InterruptedException {
        LongAdder readLocks = new LongAdder();
        LongAdder writeLocks = new LongAdder();
        LongAdder failedLocks = new LongAdder();

        doTestSingleKeyMultithreaded(5_000, readLocks, writeLocks, failedLocks, 2);
    }

    @Test
    public void testLockUpgrade() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        Object key = new String("test");

        lockManager.tryAcquireShared(key, txId1).join();

        lockManager.tryAcquireShared(key, txId0).join();

        CompletableFuture<Void> fut = lockManager.tryAcquire(key, txId1);
        assertFalse(fut.isDone());

        lockManager.tryReleaseShared(key, txId0);

        fut.join();

        lockManager.tryRelease(key, txId1);

        assertTrue(lockManager.queue(key).isEmpty());
    }

    @Test
    public void testLockUpgrade2() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        Object key = new String("test");

        lockManager.tryAcquireShared(key, txId0).join();

        lockManager.tryAcquireShared(key, txId1).join();

        expectConflict(lockManager.tryAcquire(key, txId0));
    }

    @Test
    public void testLockUpgrade3() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();
        Object key = new String("test");

        lockManager.tryAcquireShared(key, txId1).join();

        lockManager.tryAcquireShared(key, txId0).join();

        lockManager.tryAcquireShared(key, txId2).join();

        try {
            lockManager.tryAcquire(key, txId1).join();
        } catch (CompletionException e) {
            // Expected.
        }
    }

    @Test
    public void testLockUpgrade4() throws LockException {
        UUID txId0 = Timestamp.nextVersion().toUuid();
        UUID txId1 = Timestamp.nextVersion().toUuid();
        Object key = new String("test");

        lockManager.tryAcquireShared(key, txId1).join();

        lockManager.tryAcquireShared(key, txId0).join();

        CompletableFuture<Void> fut = lockManager.tryAcquire(key, txId1);

        assertFalse(fut.isDone());

        lockManager.tryReleaseShared(key, txId0);

        fut.join();

        assertTrue(lockManager.queue(key).size() == 1);
    }

    @Test
    public void testReenter() throws LockException {
        UUID txId = Timestamp.nextVersion().toUuid();
        Object key = new String("test");

        CompletableFuture<Void> fut = lockManager.tryAcquire(key, txId);
        assertTrue(fut.isDone());

        fut = lockManager.tryAcquire(key, txId);
        assertTrue(fut.isDone());

        assertTrue(lockManager.queue(key).size() == 1);

        lockManager.tryRelease(key, txId);

        assertTrue(lockManager.queue(key).isEmpty());

        fut = lockManager.tryAcquireShared(key, txId);
        assertTrue(fut.isDone());

        fut = lockManager.tryAcquireShared(key, txId);
        assertTrue(fut.isDone());

        assertTrue(lockManager.queue(key).size() == 1);

        lockManager.tryReleaseShared(key, txId);

        assertTrue(lockManager.queue(key).isEmpty());
    }

    /**
     * Do test single key multithreaded.
     *
     * @param duration The duration.
     * @param readLocks Read lock accumulator.
     * @param writeLocks Write lock accumulator.
     * @param failedLocks Failed lock accumulator.
     * @param mode Mode: 0 - read only, 1 - write only, 2 - mixed random.
     * @throws InterruptedException If interrupted while waiting.
     */
    private void doTestSingleKeyMultithreaded(
            long duration,
            LongAdder readLocks,
            LongAdder writeLocks,
            LongAdder failedLocks,
            int mode
    ) throws InterruptedException {
        Object key = new String("test");

        Thread[] threads = new Thread[Runtime.getRuntime().availableProcessors() * 2];

        CyclicBarrier startBar = new CyclicBarrier(threads.length, () -> log.info("Before test"));

        AtomicBoolean stop = new AtomicBoolean();

        Random r = new Random();

        AtomicReference<Throwable> firstErr = new AtomicReference<>();

        try {
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        startBar.await();
                    } catch (Exception e) {
                        fail();
                    }

                    while (!stop.get() && firstErr.get() == null) {
                        UUID txId = Timestamp.nextVersion().toUuid();

                        if (mode == 0 ? false : mode == 1 ? true : r.nextBoolean()) {
                            try {
                                lockManager.tryAcquire(key, txId).join();

                                writeLocks.increment();
                            } catch (CompletionException e) {
                                failedLocks.increment();
                                continue;
                            }

                            try {
                                lockManager.tryRelease(key, txId);
                            } catch (LockException e) {
                                fail(e.getMessage());
                            }
                        } else {
                            try {
                                lockManager.tryAcquireShared(key, txId).join();

                                readLocks.increment();
                            } catch (CompletionException e) {
                                if (mode == 0) {
                                    fail("Unexpected exception for read only locking mode");
                                }

                                failedLocks.increment();

                                continue;
                            }

                            try {
                                lockManager.tryReleaseShared(key, txId);
                            } catch (LockException e) {
                                fail(e.getMessage());
                            }
                        }
                    }
                });

                threads[i].setName("Worker" + i);

                threads[i].setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        firstErr.compareAndExchange(null, e);
                    }
                });

                threads[i].start();
            }

            Thread.sleep(duration);

            stop.set(true);
        } finally {
            for (Thread thread : threads) {
                thread.join();
            }
        }

        if (firstErr.get() != null) {
            throw new IgniteException(firstErr.get());
        }

        log.info("After test readLocks={} writeLocks={} failedLocks={}", readLocks.sum(), writeLocks.sum(),
                failedLocks.sum());

        assertTrue(lockManager.queue(key).isEmpty());
    }

    private UUID[] generate(int num) {
        UUID[] tmp = new UUID[num];

        for (int i = 0; i < tmp.length; i++) {
            tmp[i] = Timestamp.nextVersion().toUuid();
        }

        for (int i = 1; i < tmp.length; i++) {
            assertTrue(tmp[i - 1].compareTo(tmp[i]) < 0);
        }

        return tmp;
    }

    private void expectConflict(CompletableFuture<Void> fut) {
        try {
            fut.join();

            fail();
        } catch (CompletionException e) {
            assertTrue(IgniteTestUtils.hasCause(e, LockException.class, null),
                    "Wrong exception type, expecting LockException");
        }
    }
}
