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

package org.apache.ignite.internal.pagememory.util;

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.stream.IntStream;
import org.apache.ignite.internal.lang.IgniteInternalException;

/**
 * Helper class to test blocking data structures, with a primary goal of detecting and reproducing issues in concurrent algorithms. Allows
 * to reliably repeat the specific sequence of concurrent operations in tests.
 *
 * <p>Typical usage pattern looks like this:
 * <pre><code>
 *     int threads = ...;
 *     long seed = ...;
 *
 *     Random random = new Random(seed);
 *     Sequencer sequencer = new Sequencer(() -> random.nextInt(threads), threads);
 *
 *     for (int i = 0; i < threads; i++) {
 *         int threadId = i;
 *         Thread thread = new Thread(() -> {
 *             sequencer.setCurrentThreadId(threadId);
 *
 *             try {
 *                 for (int j = 0; j < 5; j++) {
 *                     sequencer.await();
 *
 *                     try {
 *                         System.out.println("Thread " + threadId + " is doing its thing.");
 *                     } finally {
 *                         sequencer.release();
 *                     }
 *                 }
 *             } finally {
 *                 sequencer.complete();
 *             }
 *         });
 *
 *         thread.start();
 *     }
 * </code></pre>
 *
 * <p>For a sequence {@code [0, 1, 1, 0, 1, 0, ...]} it must always produce the output
 * <pre>
 * Thread 0 is doing its thing.
 * Thread 1 is doing its thing.
 * Thread 1 is doing its thing.
 * Thread 0 is doing its thing.
 * Thread 1 is doing its thing.
 * Thread 0 is doing its thing.
 * ...
 * </pre>
 */
public class Sequencer {
    /** Thread ID used to signify that all participant threads have completed their operations. */
    private static final int TERMINATE = -1;

    /** Thread-local thread ID. Used to avoid having {@code currentThreadId} parameters in API. */
    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<Integer> currentThreadId = new ThreadLocal<>();

    /** Mutex for synchronizing the {@link #nextThreadId} access and notifying waiting threads. */
    private final Object mux = new Object();

    /** Infinite iterator to generate the sequence of threads. */
    private final PrimitiveIterator.OfInt threadIdGenerator;

    /**
     * ID of the next thread that will successfully enter its critical section. If that thread is already completed its task, this value
     * will be updated until alive thread is found or sequencer is terminated. Should only be accessed while holding {@link #mux}.
     */
    private int nextThreadId;

    /**
     * The number of threads that have not called {@link #complete()} yet. If it's {@code 0} then next thread ID is set to
     * {@link #TERMINATE} and all {@link #complete()} calls are unblocked.
     */
    private final AtomicInteger notCompleted = new AtomicInteger();

    /**
     * Constructor.
     *
     * @param threadIdGenerator Thread ID supplier to generates a sequence, in which threads must hold their critical sections.
     * @param threads Total number of threads used.
     */
    public Sequencer(IntSupplier threadIdGenerator, int threads) {
        this.threadIdGenerator = IntStream.generate(threadIdGenerator).iterator();
        this.nextThreadId = this.threadIdGenerator.nextInt(); // Without mutex, we rely on safe publication of the instance itself.
        this.notCompleted.set(threads);
    }

    /**
     * Sets current thread ID thread-locally. This value will be forgotten once {@link #complete()} is called.
     *
     * @param currentThreadId Current thread ID.
     */
    public void setCurrentThreadId(int currentThreadId) {
        assert currentThreadId > TERMINATE : currentThreadId;
        assert this.currentThreadId.get() == null : this.currentThreadId.get();

        this.currentThreadId.set(currentThreadId);
    }

    /**
     * Returns current thread ID.
     *
     * @throws NullPointerException If {@link #setCurrentThreadId(int)} hasn't been called.
     */
    public int getCurrentThreadId() {
        return Objects.requireNonNull(this.currentThreadId.get(), "currentThreadId");
    }

    /**
     * Enters the critical section. Thread will wait for its turn according to the sequence passed to constructor.
     *
     * @throws NullPointerException If {@link #setCurrentThreadId(int)} hasn't been called.
     * @throws IgniteInternalException If thread has been interrupted. Corresponding {@link InterruptedException} will be its cause.
     */
    public void await() {
        int currentThreadId = getCurrentThreadId();

        boolean res = await0(currentThreadId);

        assert res;
    }

    /**
     * Exits the critical section. This action notifies the next thread in the sequence that it's its turn.
     */
    public void release() {
        synchronized (mux) {
            if (nextThreadId != TERMINATE) {
                nextThreadId = threadIdGenerator.nextInt();

                assert nextThreadId > TERMINATE;
            }

            mux.notifyAll();
        }
    }

    /**
     * Signifies that current thread doesn't need to enter critical section anymore. If there's an ID of current thread in the sequence,
     * it'll be ignored. This methods blocks the thread until all threads call {@link #complete()} as well, only then all threads will be
     * unlocked.
     *
     * @throws NullPointerException If {@link #setCurrentThreadId(int)} hasn't been called.
     * @throws IgniteInternalException If thread has been interrupted. Corresponding {@link InterruptedException} will be its cause.
     */
    public void complete() {
        int currentThreadId = getCurrentThreadId();
        this.currentThreadId.remove();

        if (notCompleted.decrementAndGet() == 0) {
            synchronized (mux) {
                nextThreadId = TERMINATE;

                mux.notifyAll();

                return;
            }
        }

        while (await0(currentThreadId)) {
            release();
        }
    }

    private boolean await0(int currentThreadId) {
        synchronized (mux) {
            while (true) {
                int next = nextThreadId;

                if (next == TERMINATE) {
                    return false;
                }

                if (next == currentThreadId) {
                    return true;
                }

                try {
                    //noinspection WaitOrAwaitWithoutTimeout
                    mux.wait();
                } catch (InterruptedException e) {
                    throw new IgniteInternalException(INTERNAL_ERR,  e);
                }
            }
        }
    }
}
