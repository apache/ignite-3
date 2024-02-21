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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** For {@link AbstractEventProducer} testing. */
public class EventProducerTest {
    @Test
    public void simpleAsyncTest() {
        AbstractEventProducer<TestEvent, TestEventParameters> producer = new AbstractEventProducer<>() {};

        CompletableFuture<Boolean> future = new CompletableFuture<>();

        producer.listen(TestEvent.TEST, p -> future);

        CompletableFuture<?> eventHandleFuture = producer.fireEvent(TestEvent.TEST, new TestEventParameters(0L));

        assertFalse(eventHandleFuture.isDone());

        future.complete(true);

        assertTrue(eventHandleFuture.isDone());
    }

    @Test
    public void stopListenTest() {
        AbstractEventProducer<TestEvent, TestEventParameters> producer = new AbstractEventProducer<>() {};

        final int stopListenAfterCount = 5;
        final int fireEventCount = stopListenAfterCount * 2;
        AtomicInteger listenCount = new AtomicInteger();

        EventListener<TestEventParameters> listener = p -> completedFuture(listenCount.incrementAndGet() == stopListenAfterCount);

        producer.listen(TestEvent.TEST, listener);

        for (int i = 0; i < fireEventCount; i++) {
            producer.fireEvent(TestEvent.TEST, new TestEventParameters(0L));
        }

        assertEquals(stopListenAfterCount, listenCount.get());
    }

    @Test
    public void parallelTest() {
        AbstractEventProducer<TestEvent, TestEventParameters> producer = new AbstractEventProducer<>() {};

        final int listenersCount = 10_000;
        final int listenerIndexToRemove = 10;
        EventListener<TestEventParameters> listenerToRemove = null;

        CompletableFuture<Void> toRemoveFuture = new CompletableFuture<>();

        for (int i = 0; i < listenersCount; i++) {
            EventListener<TestEventParameters> listener;

            if (i == listenerIndexToRemove) {
                listener = p -> {
                    toRemoveFuture.complete(null);

                    return falseCompletedFuture();
                };

                listenerToRemove = listener;
            } else {
                listener = p -> falseCompletedFuture();
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

    private enum TestEvent implements Event {
        TEST
    }

    private static class TestEventParameters extends CausalEventParameters {
        private TestEventParameters(long causalityToken) {
            super(causalityToken);
        }
    }
}
