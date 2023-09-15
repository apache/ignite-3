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

package org.apache.ignite.internal.event;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

/** For {@link EventProducer} testing. */
public class EventProducerTest {
    @Test
    public void simpleAsyncTest() {
        EventProducer<TestEvent, TestEventParameters> producer = new EventProducer<>() {
        };

        CompletableFuture<Boolean> future = new CompletableFuture<>();

        producer.listen(TestEvent.TEST, (p, e) -> future);

        CompletableFuture<?> eventHandleFuture = producer.fireEvent(TestEvent.TEST, new TestEventParameters(0L));

        assertThat(eventHandleFuture, willTimeoutFast());

        future.complete(true);

        assertThat(eventHandleFuture, willCompleteSuccessfully());
    }

    @Test
    public void stopListenTest() {
        EventProducer<TestEvent, TestEventParameters> producer = new EventProducer<>() {};

        final int stopListenAfterCount = 5;
        final int fireEventCount = stopListenAfterCount * 2;
        AtomicInteger listenConter = new AtomicInteger();

        EventListener<TestEventParameters> listener = createEventListener(
                (p, e) -> completedFuture(listenConter.incrementAndGet() == stopListenAfterCount)
        );

        producer.listen(TestEvent.TEST, listener);

        for (int i = 0; i < fireEventCount; i++) {
            producer.fireEvent(TestEvent.TEST, new TestEventParameters(0L));
        }

        assertEquals(stopListenAfterCount, listenConter.get());
    }

    @Test
    public void parallelTest() {
        EventProducer<TestEvent, TestEventParameters> producer = new EventProducer<>() {};

        final int listenersCount = 10_000;
        final int listenerIndexToRemove = 10;
        EventListener<TestEventParameters> listenerToRemove = null;

        CompletableFuture<Void> toRemoveFuture = new CompletableFuture<>();

        for (int i = 0; i < listenersCount; i++) {
            EventListener<TestEventParameters> listener = i == listenerIndexToRemove
                    ? createEventListener(
                    (p, e) -> {
                        toRemoveFuture.complete(null);

                        return completedFuture(false);
                    }
            )
                    : createEventListener((p, e) -> completedFuture(false));

            if (i == listenerIndexToRemove) {
                listenerToRemove = listener;
            }

            producer.listen(TestEvent.TEST, listener);
        }

        CompletableFuture<Void> fireFuture = runAsync(() -> assertThat(
                producer.fireEvent(TestEvent.TEST, new TestEventParameters(0L)),
                willCompleteSuccessfully()
        ));

        assertThat(toRemoveFuture, willCompleteSuccessfully());

        producer.removeListener(TestEvent.TEST, listenerToRemove);

        assertThat(fireFuture, willCompleteSuccessfully());
    }

    private static <T extends EventParameters> EventListener<T> createEventListener(
            BiFunction<T, Throwable, CompletableFuture<Boolean>> notify
    ) {
        return notify::apply;
    }

    private enum TestEvent implements Event {
        TEST
    }

    private static class TestEventParameters extends EventParameters {
        private TestEventParameters(long causalityToken) {
            super(causalityToken);
        }
    }
}
