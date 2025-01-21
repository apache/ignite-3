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

package org.apache.ignite.internal.pagememory.tree.util;

import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;
import org.apache.ignite.internal.pagememory.util.SequencedOffheapReadWriteLock;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SequencedOffheapReadWriteLock}.
 */
class SequencedOffheapReadWriteLockTest {
    private static final int TAG = 1;

    private SequencedOffheapReadWriteLock lock = new SequencedOffheapReadWriteLock();

    private long addr;
    private long addr2;

    @BeforeEach
    void setUp() {
        addr = GridUnsafe.allocateMemory(OffheapReadWriteLock.LOCK_SIZE * 2);
        addr2 = addr + OffheapReadWriteLock.LOCK_SIZE;

        lock.init(addr, TAG);
        lock.init(addr2, TAG);
    }

    @AfterEach
    void tearDown() {
        GridUnsafe.freeMemory(addr);
    }

    @Test
    void testReadLockAndUnlock() {
        lock.startSequencing(() -> 0, 1);
        lock.setCurrentThreadId(0);

        assertTrue(lock.readLock(addr, 1));
        lock.readUnlock(addr);
    }

    @Test
    void testWriteLockAndUnlock() {
        lock.startSequencing(() -> 0, 1);
        lock.setCurrentThreadId(0);

        assertTrue(lock.writeLock(addr, 1));
        lock.writeUnlock(addr, 1);
    }

    @Test
    void testSequencing() throws InterruptedException {
        int threads = 4;
        int batch = 10;
        CountDownLatch latch = new CountDownLatch(threads);

        List<Integer> sequence = IntStream.range(0, threads)
                .mapToObj(threadId -> nCopies(batch, threadId))
                .flatMap(Collection::stream)
                .collect(toList());

        Collections.shuffle(sequence);

        IntSupplier threadIdGenerator = iterate(sequence, 0);

        lock.startSequencing(threadIdGenerator, threads);

        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            int threadId = i;
            new Thread(() -> {
                lock.setCurrentThreadId(threadId);
                try {
                    for (int j = 0; j < batch; j++) {
                        try {
                            if (!lock.readLock(addr, TAG)) {
                                break;
                            }

                            result.add(threadId);
                        } finally {
                            lock.readUnlock(addr);
                        }
                    }
                } finally {
                    lock.complete();
                    latch.countDown();
                }
            }).start();
        }

        latch.await();

        assertEquals(sequence, result);
    }

    @Test
    void testDeadlock() {
        lock.startSequencing(iterate(List.of(
                0, // Lock "addr". Wait for "addr2".
                1, // Try locking "addr". Fail. No deadlock.
                0, // Lock "addr2". Unlock "addr" and "addr2". Wait for "complete".
                1 // Try locking "addr". Succeed. Unlock "addr". Complete.
        ), 0), 2);

        IgniteTestUtils.runRace(
                () -> {
                    lock.setCurrentThreadId(0);

                    lock.readLock(addr, TAG);
                    lock.readLock(addr2, TAG);

                    lock.readUnlock(addr2);
                    lock.readUnlock(addr);

                    lock.complete();
                },
                () -> {
                    lock.setCurrentThreadId(1);

                    lock.readLock(addr, TAG);
                    lock.readUnlock(addr);

                    lock.complete();
                }
        );
    }

    private static IntSupplier iterate(List<Integer> sequence, int dflt) {
        Iterator<Integer> iterator = sequence.iterator();

        return () -> iterator.hasNext() ? iterator.next() : dflt;
    }
}
