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

package org.apache.ignite.internal.manager;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test of some implementation of {@link Producer} and {@link EventListener}.
 */
public class ProducerTest {
    @Test
    public void simpleAsyncTest() {
        Producer<TestEvent, TestEventParameters> producer = new Producer<>() {
        };

        CompletableFuture<Boolean> future = new CompletableFuture<>();

        producer.listen(TestEvent.TEST, (p, e) -> future);

        CompletableFuture<?> eventHandleFuture = producer.fireEvent(TestEvent.TEST, new TestEventParameters(0L));

        assertFalse(eventHandleFuture.isDone());

        future.complete(true);

        assertTrue(eventHandleFuture.isDone());
    }

    @Test
    public void stopListenTest() {
        Producer<TestEvent, TestEventParameters> producer = new Producer<>() {
        };

        final int stopListenAfterCount = 5;
        final int fireEventCount = stopListenAfterCount * 2;
        AtomicInteger listenConter = new AtomicInteger();
        AtomicInteger removeCounter = new AtomicInteger();

        EventListener<TestEventParameters> listener = createEventListener(
                (p, e) -> completedFuture(listenConter.incrementAndGet() == stopListenAfterCount),
                t -> removeCounter.incrementAndGet()
        );

        producer.listen(TestEvent.TEST, listener);

        for (int i = 0; i < fireEventCount; i++) {
            producer.fireEvent(TestEvent.TEST, new TestEventParameters(0L));
        }

        assertEquals(stopListenAfterCount, listenConter.get());
        assertEquals(0, removeCounter.get());
    }

    @Test
    public void parallelTest() {
        Producer<TestEvent, TestEventParameters> producer = new Producer<>() {
        };

        final int listenersCount = 10_000;
        final int listenerIndexToRemove = 10;
        EventListener<TestEventParameters> listenerToRemove = null;

        CompletableFuture toRemoveFuture = new CompletableFuture();

        for (int i = 0; i < listenersCount; i++) {
            EventListener<TestEventParameters> listener = i == listenerIndexToRemove
                    ? createEventListener(
                        (p, e) -> {
                            toRemoveFuture.complete(null);

                            return completedFuture(false);
                        },
                        t -> {}
                    )
                    : createEventListener((p, e) -> completedFuture(false), t -> {});

            if (i == listenerIndexToRemove) {
                listenerToRemove = listener;
            }

            producer.listen(TestEvent.TEST, listener);
        }

        CompletableFuture fireFuture = runAsync(() -> producer.fireEvent(TestEvent.TEST, new TestEventParameters(0L)).join());

        toRemoveFuture.join();

        producer.removeListener(TestEvent.TEST, listenerToRemove);

        fireFuture.join();

        assertFalse(fireFuture.isCompletedExceptionally());
    }

    /**
     * Create event listener.
     *
     * @param notify Notify action.
     * @param remove Remove action.
     * @return Listener.
     */
    private <T extends EventParameters> EventListener<T> createEventListener(
            BiFunction<T, Throwable, CompletableFuture<Boolean>> notify,
            Consumer<Throwable> remove
    ) {
        return new EventListener<T>() {
            @Override public CompletableFuture<Boolean> notify(T parameters, @Nullable Throwable exception) {
                return notify.apply(parameters, exception);
            }

            @Override public void remove(Throwable throwable) {
                remove.accept(throwable);
            }
        };
    }

    /**
     * Test event.
     */
    private enum TestEvent implements Event {
        TEST
    }

    /**
     * Parameters for test event.
     */
    private class TestEventParameters extends EventParameters {
        /**
         * Constructor.
         *
         * @param causalityToken Causality token.
         */
        public TestEventParameters(long causalityToken) {
            super(causalityToken);
        }
    }
}
