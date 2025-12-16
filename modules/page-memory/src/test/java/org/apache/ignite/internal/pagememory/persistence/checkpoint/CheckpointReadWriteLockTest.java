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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link CheckpointReadWriteLock} testing.
 */
@ExtendWith(ExecutorServiceExtension.class)
public class CheckpointReadWriteLockTest {
    @InjectExecutorService
    private ExecutorService executorService;

    private final CheckpointReadWriteLockMetricSource metricSource = new CheckpointReadWriteLockMetricSource();

    @Test
    void testReadLock() throws Exception {
        CheckpointReadWriteLock lock0 = newReadWriteLock();
        CheckpointReadWriteLock lock1 = newReadWriteLock();

        lock1.writeLock();

        lock0.readLock();
        lock1.readLock();

        assertEquals(1, lock0.getReadHoldCount());
        assertEquals(0, lock1.getReadHoldCount());

        lock1.writeUnlock();

        runAsync(() -> {
            assertEquals(0, lock0.getReadHoldCount());
            assertEquals(0, lock1.getReadHoldCount());

            lock0.readLock();
            lock1.readLock();

            assertEquals(1, lock0.getReadHoldCount());
            assertEquals(1, lock1.getReadHoldCount());

            lock0.readUnlock();
            lock1.readUnlock();

            assertEquals(0, lock0.getReadHoldCount());
            assertEquals(0, lock1.getReadHoldCount());
        }).get(1, TimeUnit.SECONDS);

        lock1.writeLock();

        assertEquals(1, lock0.getReadHoldCount());
        assertEquals(0, lock1.getReadHoldCount());

        lock0.readUnlock();
        lock1.readUnlock();

        assertEquals(0, lock0.getReadHoldCount());
        assertEquals(0, lock1.getReadHoldCount());
    }

    @Test
    void testTryReadLock() throws Exception {
        CheckpointReadWriteLock lock0 = newReadWriteLock();
        CheckpointReadWriteLock lock1 = newReadWriteLock();
        CheckpointReadWriteLock lock2 = newReadWriteLock();

        lock2.writeLock();

        assertTrue(lock0.tryReadLock());
        assertTrue(lock1.tryReadLock(1, TimeUnit.MILLISECONDS));

        assertTrue(lock2.tryReadLock());
        assertTrue(lock2.tryReadLock(1, TimeUnit.MILLISECONDS));

        assertEquals(1, lock0.getReadHoldCount());
        assertEquals(1, lock1.getReadHoldCount());
        assertEquals(0, lock2.getReadHoldCount());

        runAsync(() -> {
            assertEquals(0, lock0.getReadHoldCount());
            assertEquals(0, lock1.getReadHoldCount());
            assertEquals(0, lock2.getReadHoldCount());

            assertFalse(lock2.tryReadLock());

            try {
                assertFalse(lock2.tryReadLock(1, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                fail(e);
            }

            assertEquals(0, lock0.getReadHoldCount());
            assertEquals(0, lock1.getReadHoldCount());
            assertEquals(0, lock2.getReadHoldCount());
        }).get(1, TimeUnit.SECONDS);

        lock2.writeUnlock();

        runAsync(() -> {
            try {
                assertTrue(lock0.tryReadLock());
                assertTrue(lock1.tryReadLock(1, TimeUnit.MILLISECONDS));

                assertTrue(lock2.tryReadLock());
                assertTrue(lock2.tryReadLock(1, TimeUnit.MILLISECONDS));

                assertEquals(1, lock0.getReadHoldCount());
                assertEquals(1, lock1.getReadHoldCount());
                assertEquals(2, lock2.getReadHoldCount());

                lock0.readUnlock();
                lock1.readUnlock();
                lock2.readUnlock();

                assertEquals(0, lock0.getReadHoldCount());
                assertEquals(0, lock1.getReadHoldCount());
                assertEquals(1, lock2.getReadHoldCount());

                lock2.readUnlock();

                assertEquals(0, lock2.getReadHoldCount());
            } catch (InterruptedException e) {
                fail(e);
            }
        }).get(1, TimeUnit.SECONDS);

        lock2.writeLock();

        assertEquals(1, lock0.getReadHoldCount());
        assertEquals(1, lock1.getReadHoldCount());
        assertEquals(0, lock2.getReadHoldCount());

        lock0.readUnlock();
        lock1.readUnlock();
        lock2.readUnlock();

        assertEquals(0, lock0.getReadHoldCount());
        assertEquals(0, lock1.getReadHoldCount());
        assertEquals(0, lock2.getReadHoldCount());
    }

    private CheckpointReadWriteLock newReadWriteLock() {
        return new CheckpointReadWriteLock(new ReentrantReadWriteLockWithTracking(), executorService, metricSource);
    }

    @Test
    void testCheckpointLockIsHeldByThread() throws Exception {
        CheckpointReadWriteLock lock0 = newReadWriteLock();
        CheckpointReadWriteLock lock1 = newReadWriteLock();
        CheckpointReadWriteLock lock2 = newReadWriteLock();

        assertFalse(lock0.checkpointLockIsHeldByThread());
        assertFalse(lock1.checkpointLockIsHeldByThread());
        assertFalse(lock2.checkpointLockIsHeldByThread());

        lock0.writeLock();
        lock1.readLock();

        assertTrue(lock0.checkpointLockIsHeldByThread());
        assertTrue(lock1.checkpointLockIsHeldByThread());

        assertThat(
                runAsync(() -> assertFalse(lock2.checkpointLockIsHeldByThread()), "checkpoint-runner"),
                willCompleteSuccessfully()
        );

        assertThat(
                runInCheckpointThreadAsync(() -> assertTrue(lock2.checkpointLockIsHeldByThread())),
                willCompleteSuccessfully()
        );

        runAsync(() -> {
            assertFalse(lock0.checkpointLockIsHeldByThread());
            assertFalse(lock1.checkpointLockIsHeldByThread());
            assertFalse(lock2.checkpointLockIsHeldByThread());
        }).get(1, TimeUnit.SECONDS);

        runAsync(() -> {
            assertFalse(lock0.tryReadLock());

            try {
                assertFalse(lock0.tryReadLock(1, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                fail(e);
            }

            assertFalse(lock0.checkpointLockIsHeldByThread());
        }).get(1, TimeUnit.SECONDS);

        lock0.writeUnlock();
        lock1.readUnlock();

        assertFalse(lock0.checkpointLockIsHeldByThread());
        assertFalse(lock1.checkpointLockIsHeldByThread());
        assertFalse(lock2.checkpointLockIsHeldByThread());
    }

    private static CompletableFuture<Void> runInCheckpointThreadAsync(Runnable r) {
        var future = new CompletableFuture<Void>();

        var thread = new IgniteCheckpointThread("test", "test", () -> {
            try {
                r.run();

                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });

        thread.start();

        return future;
    }
}
