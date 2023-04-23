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

package org.apache.ignite.internal.util.worker;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.internal.util.worker.IgniteWorkerTest.TestWorkerListener.ON_IDLE;
import static org.apache.ignite.internal.util.worker.IgniteWorkerTest.TestWorkerListener.ON_STARTED;
import static org.apache.ignite.internal.util.worker.IgniteWorkerTest.TestWorkerListener.ON_STOPPED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * For {@link IgniteWorker} testing.
 */
public class IgniteWorkerTest {
    static final String CLEANUP = "cleanup";

    private final IgniteLogger log = Loggers.forClass(IgniteWorkerTest.class);

    @Test
    void testNewIgniteWorker() {
        IgniteWorker worker = new NoopWorker(log, null);

        assertEquals("testNode", worker.igniteInstanceName());
        assertEquals("testWorker", worker.name());

        assertEquals(0, worker.heartbeat());

        assertFalse(worker.isCancelled());
        assertFalse(worker.isDone());

        assertNull(worker.runner());
    }

    @Test
    void testBlockingSelection() {
        IgniteWorker worker = new NoopWorker(log, null);

        long currentTimeMillis = coarseCurrentTimeMillis();

        worker.blockingSectionBegin();

        assertEquals(Long.MAX_VALUE, worker.heartbeat());

        worker.blockingSectionEnd();

        assertThat(worker.heartbeat(), greaterThanOrEqualTo(currentTimeMillis));

        // Checks update heartbeat after blockingSectionBegin().

        worker.blockingSectionBegin();

        assertEquals(Long.MAX_VALUE, worker.heartbeat());

        worker.updateHeartbeat();

        assertThat(worker.heartbeat(), greaterThanOrEqualTo(currentTimeMillis));

        worker.blockingSectionEnd();

        assertThat(worker.heartbeat(), greaterThanOrEqualTo(currentTimeMillis));
    }

    @Test
    void testUpdateHeartbeat() throws Exception {
        IgniteWorker worker = new NoopWorker(log, null);

        assertEquals(0, worker.heartbeat());

        long coarseCurrentTimeMillis = coarseCurrentTimeMillis();

        worker.updateHeartbeat();

        long heartbeat = worker.heartbeat();

        assertThat(heartbeat, greaterThanOrEqualTo(coarseCurrentTimeMillis));

        assertTrue(waitForCondition(() -> coarseCurrentTimeMillis() > heartbeat, 10, 1_000));

        worker.updateHeartbeat();

        assertThat(worker.heartbeat(), greaterThan(heartbeat));
        assertThat(worker.heartbeat(), greaterThan(coarseCurrentTimeMillis));
        assertThat(worker.heartbeat(), lessThanOrEqualTo(coarseCurrentTimeMillis()));
    }

    @Test
    void testIdle() {
        List<String> events = new ArrayList<>();

        TestWorkerListener listener = new TestWorkerListener(events);

        IgniteWorker worker = new NoopWorker(log, listener);

        worker.onIdle();

        assertThat(events, equalTo(List.of(ON_IDLE)));
    }

    @Test
    void testRun() {
        List<String> events = new ArrayList<>();

        TestWorkerListener listener = new TestWorkerListener(events) {
            /** {@inheritDoc} */
            @Override
            public void onStarted(IgniteWorker worker) {
                super.onStarted(worker);

                assertThat(worker.heartbeat(), lessThanOrEqualTo(coarseCurrentTimeMillis()));
                assertSame(Thread.currentThread(), worker.runner());
                assertFalse(worker.isCancelled());
                assertFalse(worker.isDone());
            }

            /** {@inheritDoc} */
            @Override
            public void onStopped(IgniteWorker worker) {
                super.onStopped(worker);

                assertThat(worker.heartbeat(), lessThanOrEqualTo(coarseCurrentTimeMillis()));
                assertSame(Thread.currentThread(), worker.runner());
                assertFalse(worker.isCancelled());
                assertTrue(worker.isDone());
            }
        };

        IgniteWorker worker = new NoopWorker(log, listener) {
            /** {@inheritDoc} */
            @Override
            protected void cleanup() {
                events.add(CLEANUP);
            }
        };

        worker.run();

        assertThat(events, equalTo(List.of(ON_STARTED, CLEANUP, ON_STOPPED)));

        assertThat(worker.heartbeat(), lessThanOrEqualTo(coarseCurrentTimeMillis()));
        assertNull(worker.runner());
        assertFalse(worker.isCancelled());
        assertTrue(worker.isDone());
    }

    @Test
    void testInterruptFromBody() {
        List<String> events = new ArrayList<>();

        TestWorkerListener listener = new TestWorkerListener(events);

        IgniteWorker worker = new NoopWorker(log, listener) {
            /** {@inheritDoc} */
            @Override
            protected void body() throws InterruptedException {
                Thread.currentThread().interrupt();

                throw new InterruptedException();
            }

            /** {@inheritDoc} */
            @Override
            protected void cleanup() {
                events.add(CLEANUP);
            }
        };

        worker.run();

        assertThat(listener.events, equalTo(List.of(ON_STARTED, CLEANUP, ON_STOPPED)));

        assertThat(worker.heartbeat(), lessThanOrEqualTo(coarseCurrentTimeMillis()));
        assertNull(worker.runner());
        assertFalse(worker.isCancelled());
        assertTrue(worker.isDone());
        assertTrue(Thread.interrupted());
    }

    @Test
    void testExceptionFromBody() {
        List<String> events = new ArrayList<>();

        TestWorkerListener listener = new TestWorkerListener(events);

        IgniteWorker worker = new NoopWorker(log, listener) {
            /** {@inheritDoc} */
            @Override
            protected void body() {
                throw new RuntimeException();
            }

            /** {@inheritDoc} */
            @Override
            protected void cleanup() {
                events.add(CLEANUP);
            }
        };

        worker.run();

        assertThat(listener.events, equalTo(List.of(ON_STARTED, CLEANUP, ON_STOPPED)));

        assertThat(worker.heartbeat(), lessThanOrEqualTo(coarseCurrentTimeMillis()));
        assertNull(worker.runner());
        assertFalse(worker.isCancelled());
        assertTrue(worker.isDone());
    }

    @Test
    void testErrorFromBody() {
        List<String> events = new ArrayList<>();

        TestWorkerListener listener = new TestWorkerListener(events);

        IgniteWorker worker = new NoopWorker(log, listener) {
            /** {@inheritDoc} */
            @Override
            protected void body() {
                throw new Error();
            }

            /** {@inheritDoc} */
            @Override
            protected void cleanup() {
                events.add(CLEANUP);
            }
        };

        assertThrows(Error.class, worker::run);

        assertThat(listener.events, equalTo(List.of(ON_STARTED, CLEANUP, ON_STOPPED)));

        assertThat(worker.heartbeat(), lessThanOrEqualTo(coarseCurrentTimeMillis()));
        assertNull(worker.runner());
        assertFalse(worker.isCancelled());
        assertTrue(worker.isDone());
    }

    @Test
    void testCancelWorker() {
        List<String> events = new ArrayList<>();

        TestWorkerListener listener = new TestWorkerListener(events);

        IgniteWorker worker = new NoopWorker(log, listener) {
            /** {@inheritDoc} */
            @Override
            protected void body() {
                cancel();

                cancel();
            }

            /** {@inheritDoc} */
            @Override
            protected void cleanup() {
                events.add(CLEANUP);
            }

            /** {@inheritDoc} */
            @Override
            protected void onCancel(boolean firstCancelRequest) {
                super.onCancel(firstCancelRequest);

                events.add("firstCancel=" + firstCancelRequest);

                assertTrue(runner().isInterrupted());
                assertTrue(isCancelled());
                assertFalse(isDone());
            }
        };

        worker.run();

        assertThat(listener.events, equalTo(List.of(ON_STARTED, "firstCancel=true", "firstCancel=false", CLEANUP, ON_STOPPED)));

        assertThat(worker.heartbeat(), lessThanOrEqualTo(coarseCurrentTimeMillis()));
        assertNull(worker.runner());
        assertTrue(worker.isCancelled());
        assertTrue(worker.isDone());
        assertTrue(Thread.interrupted());
    }

    @Test
    void testCancelBeforeStartWorker() {
        List<String> events = new ArrayList<>();

        TestWorkerListener listener = new TestWorkerListener(events);

        IgniteWorker worker = new NoopWorker(log, listener) {
            /** {@inheritDoc} */
            @Override
            protected void cleanup() {
                events.add(CLEANUP);
            }

            /** {@inheritDoc} */
            @Override
            protected void onCancel(boolean firstCancelRequest) {
                super.onCancel(firstCancelRequest);

                events.add("firstCancel=" + firstCancelRequest);
            }

            /** {@inheritDoc} */
            @Override
            protected void onCancelledBeforeWorkerScheduled() {
                super.onCancelledBeforeWorkerScheduled();

                events.add("onCancelledBeforeWorkerScheduled");

                assertTrue(runner().isInterrupted());
                assertTrue(isCancelled());
                assertFalse(isDone());
            }
        };

        worker.cancel();

        worker.run();

        assertThat(
                listener.events,
                equalTo(List.of("firstCancel=true", "onCancelledBeforeWorkerScheduled", ON_STARTED, CLEANUP, ON_STOPPED))
        );

        assertThat(worker.heartbeat(), lessThanOrEqualTo(coarseCurrentTimeMillis()));
        assertNull(worker.runner());
        assertTrue(worker.isCancelled());
        assertTrue(worker.isDone());
        assertTrue(Thread.interrupted());
    }

    @Test
    void testJoin() throws Exception {
        CountDownLatch finishBodyLatch = new CountDownLatch(1);

        CountDownLatch startLatch = new CountDownLatch(2);

        try {
            IgniteWorker worker = new NoopWorker(log, null) {
                /** {@inheritDoc} */
                @Override
                protected void body() throws InterruptedException {
                    finishBodyLatch.await(1, TimeUnit.SECONDS);
                }
            };

            CompletableFuture<?> runWorkerFuture = runAsync(() -> {
                startLatch.countDown();

                worker.run();
            });

            CompletableFuture<?> joinWorkerFuture = runAsync(() -> {
                startLatch.countDown();

                worker.join();
            });

            startLatch.await(100, TimeUnit.MILLISECONDS);

            assertThrows(TimeoutException.class, () -> joinWorkerFuture.get(100, TimeUnit.MILLISECONDS));

            finishBodyLatch.countDown();

            runWorkerFuture.get(100, TimeUnit.MILLISECONDS);
            joinWorkerFuture.get(100, TimeUnit.MILLISECONDS);
        } finally {
            finishBodyLatch.countDown();
        }
    }

    /**
     * A worker implementation that does nothing.
     */
    private static class NoopWorker extends IgniteWorker {
        /**
         * Constructor.
         *
         * @param log Logger.
         * @param listener Listener for life-cycle events.
         */
        protected NoopWorker(IgniteLogger log, @Nullable IgniteWorkerListener listener) {
            super(log, "testNode", "testWorker", listener);
        }

        /** {@inheritDoc} */
        @Override
        protected void body() throws InterruptedException {
        }
    }

    /**
     * Test listener implementation that simply collects events.
     */
    static class TestWorkerListener implements IgniteWorkerListener {
        static final String ON_STARTED = "onStarted";

        static final String ON_STOPPED = "onStopped";

        static final String ON_IDLE = "onIdle";

        final List<String> events;

        /**
         * Constructor.
         *
         * @param events For recording events.
         */
        private TestWorkerListener(List<String> events) {
            this.events = events;
        }

        /** {@inheritDoc} */
        @Override
        public void onStarted(IgniteWorker worker) {
            events.add(ON_STARTED);
        }

        /** {@inheritDoc} */
        @Override
        public void onStopped(IgniteWorker worker) {
            events.add(ON_STOPPED);
        }

        /** {@inheritDoc} */
        @Override
        public void onIdle(IgniteWorker worker) {
            events.add(ON_IDLE);
        }
    }
}
