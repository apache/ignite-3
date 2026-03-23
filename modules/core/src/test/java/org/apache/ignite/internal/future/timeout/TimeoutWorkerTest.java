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

package org.apache.ignite.internal.future.timeout;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.internal.util.IgniteUtils.awaitForWorkersStop;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link TimeoutWorker}.
 */
@WithSystemProperty(key = "IGNITE_TIMEOUT_WORKER_SLEEP_INTERVAL", value = "1")
public class TimeoutWorkerTest extends BaseIgniteAbstractTest {
    private TimeoutWorker createTimeoutWorker(String name, ConcurrentMap reqMap, @Nullable FailureProcessor failureProcessor) {
        return new TimeoutWorker(log, "node", name, reqMap, failureProcessor);
    }

    @Test
    public void testTimeout() {
        ConcurrentMap<Long, TimeoutObject<?>> reqMap = new ConcurrentHashMap<>();

        CompletableFuture<Void> timeoutFuture = new CompletableFuture<>();

        TimeoutWorker worker = createTimeoutWorker("testWorker", reqMap, null);

        new IgniteThread(worker).start();

        long endTime = coarseCurrentTimeMillis() + 10;
        reqMap.put(0L, new TestTimeoutObject(() -> endTime, timeoutFuture));

        assertThat(timeoutFuture, willThrow(TimeoutException.class));
        assertTrue(reqMap.isEmpty());

        awaitForWorkersStop(List.of(worker), true, log);
    }

    @ParameterizedTest
    @MethodSource("throwableProcessors")
    public void testThrowableProcessor(@Nullable ThrowableProcessor processor) {
        ConcurrentMap<Long, TimeoutObject<?>> reqMap = new ConcurrentHashMap<>();

        RuntimeException testException = new RuntimeException("test");

        reqMap.put(0L, new TestTimeoutObject(() -> {
            throw testException;
        }, null));

        TimeoutWorker worker = createTimeoutWorker("testWorker", reqMap, processor);

        new IgniteThread(worker).start();

        if (processor != null) {
            assertThat(processor.throwableFuture, willCompleteSuccessfully());
            assertEquals(testException, processor.throwableFuture.join());
        }

        awaitForWorkersStop(List.of(worker), true, log);
    }

    private static List<ThrowableProcessor> throwableProcessors() {
        return Stream.of(new ThrowableProcessor(), null).collect(toList());
    }

    private static class ThrowableProcessor implements FailureProcessor {
        final CompletableFuture<Throwable> throwableFuture = new CompletableFuture<>();

        @Override
        public boolean process(FailureContext failureCtx) {
            throwableFuture.complete(failureCtx.error());

            return true;
        }
    }

    private static class TestTimeoutObject implements TimeoutObject<Void> {
        final Supplier<Long> endTimeSup;
        final CompletableFuture<Void> future;

        private TestTimeoutObject(Supplier<Long> endTimeSup, CompletableFuture<Void> future) {
            this.endTimeSup = endTimeSup;
            this.future = future;
        }

        @Override
        public long endTime() {
            return endTimeSup.get();
        }

        @Override
        public CompletableFuture<Void> future() {
            return future;
        }
    }
}
