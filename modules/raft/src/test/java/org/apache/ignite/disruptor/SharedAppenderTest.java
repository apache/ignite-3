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

package org.apache.ignite.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.ignite.internal.raft.storage.impl.SharedLogManagerImpl.AbstractSharedAppendQueue;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;

public class SharedAppenderTest extends IgniteAbstractTest {
    @Test
    public void testSingleThreadWriter() throws InterruptedException {
        DelayedFlushSharedQueue queue = new DelayedFlushSharedQueue();

        ConcurrentLinkedQueue<Integer> eventQueue = IgniteTestUtils.getFieldValue(queue, AbstractSharedAppendQueue.class, "queue");

        queue.append(1);
        assertEquals(1, eventQueue.size());
        CompletableFuture<Void> fut1 = queue.flushAsync();
        assertFalse(fut1.isDone());
        assertTrue(IgniteTestUtils.waitForCondition(() -> queue.futs.size() == 1, 1000));

        eventQueue = IgniteTestUtils.getFieldValue(queue, AbstractSharedAppendQueue.class, "queue");
        assertEquals(0, eventQueue.size());

        queue.append(2);
        assertEquals(1, eventQueue.size());
        CompletableFuture<Void> fut2 = queue.flushAsync();
        assertFalse(fut2.isDone());
        assertTrue(IgniteTestUtils.waitForCondition(() -> queue.futs.size() == 2, 1000));

        eventQueue = IgniteTestUtils.getFieldValue(queue, AbstractSharedAppendQueue.class, "queue");
        assertEquals(0, eventQueue.size());

        queue.append(3);
        assertEquals(1, eventQueue.size());
        CompletableFuture<Void> fut3 = queue.flushAsync();
        assertFalse(fut3.isDone());
        assertTrue(IgniteTestUtils.waitForCondition(() -> queue.futs.size() == 3, 1000));

        eventQueue = IgniteTestUtils.getFieldValue(queue, AbstractSharedAppendQueue.class, "queue");
        assertEquals(0, eventQueue.size());

        List<CompletableFuture<Void>> flushFuts = new ArrayList<>(queue.futs);

        flushFuts.get(1).complete(null);
        assertFalse(fut1.isDone());
        assertFalse(fut2.isDone());
        assertFalse(fut3.isDone());

        flushFuts.get(2).complete(null);
        assertFalse(fut1.isDone());
        assertFalse(fut2.isDone());
        assertFalse(fut3.isDone());

        flushFuts.get(0).complete(null);
        fut1.join();
        fut2.join();
        fut3.join();
    }

    @Test
    public void testSingleThreadWriterOrdered() throws InterruptedException {
        DelayedFlushSharedQueue queue = new DelayedFlushSharedQueue();

        ConcurrentLinkedQueue<Integer> eventQueue = IgniteTestUtils.getFieldValue(queue, AbstractSharedAppendQueue.class, "queue");

        queue.append(1);
        assertEquals(1, eventQueue.size());
        CompletableFuture<Void> fut1 = queue.flushAsync();
        assertFalse(fut1.isDone());
        assertTrue(IgniteTestUtils.waitForCondition(() -> queue.futs.size() == 1, 1000));

        eventQueue = IgniteTestUtils.getFieldValue(queue, AbstractSharedAppendQueue.class, "queue");
        assertEquals(0, eventQueue.size());

        queue.append(2);
        assertEquals(1, eventQueue.size());
        CompletableFuture<Void> fut2 = queue.flushAsync();
        assertFalse(fut2.isDone());
        assertTrue(IgniteTestUtils.waitForCondition(() -> queue.futs.size() == 2, 1000));

        eventQueue = IgniteTestUtils.getFieldValue(queue, AbstractSharedAppendQueue.class, "queue");
        assertEquals(0, eventQueue.size());

        queue.append(3);
        assertEquals(1, eventQueue.size());
        CompletableFuture<Void> fut3 = queue.flushAsync();
        assertFalse(fut3.isDone());
        assertTrue(IgniteTestUtils.waitForCondition(() -> queue.futs.size() == 3, 1000));

        eventQueue = IgniteTestUtils.getFieldValue(queue, AbstractSharedAppendQueue.class, "queue");
        assertEquals(0, eventQueue.size());

        List<CompletableFuture<Void>> flushFuts = new ArrayList<>(queue.futs);

        flushFuts.get(0).complete(null);
        fut1.join();
        assertFalse(fut2.isDone());
        assertFalse(fut3.isDone());

        flushFuts.get(1).complete(null);
        fut1.join();
        fut2.join();
        assertFalse(fut3.isDone());

        flushFuts.get(2).complete(null);
        fut1.join();
        fut2.join();
        fut3.join();
    }

    private class Task implements Runnable {
        static final int cnt = 10;

        final int idx;
        final int start;
        TrackingFlushSharedQueue queue;
        Executor executor;

        public Task(TrackingFlushSharedQueue queue, Executor executor, int idx, int start) {
            this.idx = idx;
            this.start = start;
            this.queue = queue;
            this.executor = executor;
        }

        @Override
        public void run() {
            if (start >= 100_000) {
                System.out.println("Done: idx=" + idx);
                return;
            }
            for (int i = 0; i < cnt; i++) {
                queue.append(new Entry(idx, start + i));
            }
            queue.flushAsync();
            executor.execute(new Task(queue, executor, idx, start + cnt));
        }
    }

    @Test
    public void testConcurrentThreadWriter() throws InterruptedException {
        TrackingFlushSharedQueue queue = new TrackingFlushSharedQueue();

        int parties = 8; // Runtime.getRuntime().availableProcessors();
        CyclicBarrier b = new CyclicBarrier(parties);

        Executor[] runners = new Executor[parties];

        for (int i = 0; i < runners.length; i++) {
            runners[i] = Executors.newFixedThreadPool(1);
        }

        // Syncer.
        for (int i = 0; i < runners.length; i++) {
            int finalI = i;
            runners[i].execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        b.await();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    Task task = new Task(queue, runners[finalI], finalI, 0);
                    task.run();
                }
            });
        }

        Thread.sleep(5000);

        // Validate log fullness.

        Map<Integer, Set<Entry>> counters = new HashMap<>();

        for (List<Entry> entries : queue.tracked) {
            for (Entry entry : entries) {
                counters.computeIfAbsent(entry.idx, (k) -> new HashSet<>()).add(entry);
            }
        }

        for (Set<Entry> value : counters.values()) {
            assertEquals(100_000, value.size());
        }
    }

    private class DelayedFlushSharedQueue extends AbstractSharedAppendQueue<Integer> {
        private final Queue<CompletableFuture<Void>> futs = new ConcurrentLinkedQueue<>();

        @Override
        protected void doFlush(Queue<Integer> queue0, CompletableFuture<Void> fut) {
            futs.add(fut);
        }
    }

    private class TrackingFlushSharedQueue extends AbstractSharedAppendQueue<Entry> {
        private final Queue<List<Entry>> tracked = new ConcurrentLinkedQueue<>();

        @Override
        protected void doFlush(Queue<Entry> queue0, CompletableFuture<Void> fut) {
            List<Entry> tmp = new ArrayList<>(queue0);
            tracked.add(tmp);
            fut.complete(null);
        }
    }

    private static final class Entry {
        final int idx;
        final  int cntr;

        public Entry(int idx, int cntr) {
            this.idx = idx;
            this.cntr = cntr;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "idx=" + idx +
                    ", cntr=" + cntr +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Entry entry = (Entry) o;

            if (idx != entry.idx) {
                return false;
            }
            return cntr == entry.cntr;
        }

        @Override
        public int hashCode() {
            int result = idx;
            result = 31 * result + cntr;
            return result;
        }
    }
}
