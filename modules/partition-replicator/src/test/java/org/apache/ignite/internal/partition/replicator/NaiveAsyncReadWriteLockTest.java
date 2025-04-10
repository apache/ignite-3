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

package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.util.CompletableFutures;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ExecutorServiceExtension.class)
class NaiveAsyncReadWriteLockTest {
    private final NaiveAsyncReadWriteLock lock = new NaiveAsyncReadWriteLock();

    @Test
    void writeLockAttemptsArrivingWhenWriteLockedGetSatisfiedWhenUnlocked() {
        List<CompletableFuture<?>> futures = new ArrayList<>();

        CompletableFuture<Long> firstFuture = lock.writeLock();

        for (int i = 0; i < 10; i++) {
            futures.add(lock.writeLock().thenAccept(lock::unlockWrite));
        }

        assertThat(firstFuture.thenAccept(lock::unlockWrite), willCompleteSuccessfully());

        assertThat(CompletableFutures.allOf(futures), willCompleteSuccessfully());
    }

    @Test
    void readLockAttemptsArrivingWhenWriteLockedGetSatisfiedWhenUnlocked() {
        List<CompletableFuture<?>> futures = new ArrayList<>();

        CompletableFuture<Long> firstFuture = lock.writeLock();

        for (int i = 0; i < 10; i++) {
            futures.add(lock.readLock().thenAccept(lock::unlockRead));
        }

        assertThat(firstFuture.thenAccept(lock::unlockWrite), willCompleteSuccessfully());

        assertThat(CompletableFutures.allOf(futures), willCompleteSuccessfully());
    }

    @Test
    void writeLockAttemptsArrivingWhenReadLockedGetSatisfiedWhenUnlocked() {
        CompletableFuture<Long> readLockFuture1 = lock.readLock();
        CompletableFuture<Long> readLockFuture2 = lock.readLock();

        CompletableFuture<Long> writeLockFuture = lock.writeLock();

        assertThat(readLockFuture1.thenAccept(lock::unlockRead), willCompleteSuccessfully());
        assertThat(readLockFuture2.thenAccept(lock::unlockRead), willCompleteSuccessfully());

        assertThat(writeLockFuture, willCompleteSuccessfully());
        assertDoesNotThrow(() -> lock.unlockWrite(writeLockFuture.join()));
    }

    @Test
    void incompleteReadUnlockDoesNotAllowWriteWaiterAcquireWriteLock() throws Exception {
        CompletableFuture<Long> readLock1 = lock.readLock();
        @SuppressWarnings("unused")
        CompletableFuture<Long> readLock2 = lock.readLock();
        CompletableFuture<Long> writeLock = lock.writeLock();

        AtomicBoolean writeLocked = new AtomicBoolean(false);
        writeLock.thenAccept(stamp -> writeLocked.set(true));

        readLock1.thenAccept(lock::unlockRead);

        assertFalse(waitForCondition(writeLocked::get, 100), "Write lock was acquired before unlocking all read locks");
    }

    @Test
    void testConcurrency(
            @InjectExecutorService(threadCount = 4) ExecutorService orchestratingExecutor,
            @InjectExecutorService(threadCount = 10) ExecutorService sleepTasksExecutor
    ) {
        List<CompletableFuture<?>> futures = new CopyOnWriteArrayList<>();

        for (int thread = 0; thread < 4; thread++) {
            orchestratingExecutor.execute(() -> {
                for (int i = 0; i < 10000; i++) {
                    CompletableFuture<?> future;

                    if (i % 2 == 0) {
                        future = lock.readLock().thenCompose(stamp -> {
                            return CompletableFuture.runAsync(() -> sleep(1), sleepTasksExecutor)
                                    .whenComplete((res, ex) -> lock.unlockRead(stamp));
                        });
                    } else {
                        future = lock.writeLock().thenCompose(stamp -> {
                            return CompletableFuture.runAsync(() -> sleep(1), sleepTasksExecutor)
                                    .whenComplete((res, ex) -> lock.unlockWrite(stamp));
                        });
                    }

                    futures.add(future);
                }
            });
        }

        assertThat(CompletableFutures.allOf(futures), willCompleteSuccessfully());
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
