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

package org.apache.ignite.internal.util;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runMultiThreaded;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for {@link PendingComparableValuesTracker}.
 */
@SuppressWarnings("rawtypes") // Because of test parametrization raw PendingComparableValuesTracker is used.
class PendingComparableValuesTrackerTest extends PendingComparableValuesTrackerTestBase {
    @Override
    protected <V extends Comparable<V>, R> void updateTracker(
            PendingComparableValuesTracker<V, R> tracker,
            V value,
            @Nullable R futureResult
    ) {
        tracker.update(value, futureResult);
    }

    @ParameterizedTest
    @MethodSource("hybridTimestampTrackerGenerator")
    public void testSimpleWaitFor(PendingComparableValuesTracker trackerParam) {
        doTestSimpleWaitFor(trackerParam);
    }

    @ParameterizedTest
    @MethodSource("hybridTimestampTrackerGenerator")
    public void testSimpleWaitForWithValue(PendingComparableValuesTracker trackerParam) {
        HybridTimestamp ts = new HybridTimestamp(1, 0);

        @SuppressWarnings("unchecked")
        PendingComparableValuesTracker<HybridTimestamp, HybridTimestamp> tracker = trackerParam;

        HybridTimestamp ts1 = new HybridTimestamp(ts.getPhysical() + 1_000_000, 0);
        HybridTimestamp ts2 = new HybridTimestamp(ts.getPhysical() + 2_000_000, 0);
        HybridTimestamp ts3 = new HybridTimestamp(ts.getPhysical() + 3_000_000, 0);

        CompletableFuture<HybridTimestamp> f0 = tracker.waitFor(ts1);
        CompletableFuture<HybridTimestamp> f1 = tracker.waitFor(ts2);
        CompletableFuture<HybridTimestamp> f2 = tracker.waitFor(ts3);

        assertFalse(f0.isDone());
        assertFalse(f1.isDone());
        assertFalse(f2.isDone());

        updateTracker(tracker, ts1, ts1);
        assertThat(f0, willBe(ts1));
        assertFalse(f1.isDone());
        assertFalse(f2.isDone());

        updateTracker(tracker, ts2, ts2);
        assertThat(f1, willBe(ts2));
        assertFalse(f2.isDone());

        updateTracker(tracker, ts3, ts3);
        assertThat(f2, willBe(ts3));
    }

    @ParameterizedTest
    @MethodSource("hybridTimestampTrackerGenerator")
    public void testMultithreadedWaitFor(PendingComparableValuesTracker trackerParam) throws Exception {
        HybridClock clock = new HybridClockImpl();

        @SuppressWarnings("unchecked")
        PendingComparableValuesTracker<HybridTimestamp, Void> tracker = trackerParam;

        int threads = Runtime.getRuntime().availableProcessors();

        List<CompletableFuture<Void>> allFutures = Collections.synchronizedList(new ArrayList<>());

        int iterations = 1_000;

        runMultiThreaded(() -> {
            NavigableMap<HybridTimestamp, CompletableFuture<Void>> prevFutures = new TreeMap<>();

            ThreadLocalRandom random = ThreadLocalRandom.current();

            for (int i = 0; i < iterations; i++) {
                HybridTimestamp now = clock.now();

                updateTracker(tracker, now, null);

                HybridTimestamp timestampToWait =
                        new HybridTimestamp(now.getPhysical() + 1, now.getLogical() + random.nextInt(1000));

                CompletableFuture<Void> future = tracker.waitFor(timestampToWait);

                prevFutures.put(timestampToWait, future);

                allFutures.add(future);

                if (i % 10 == 0) {
                    SortedMap<HybridTimestamp, CompletableFuture<Void>> beforeNow = prevFutures.headMap(now, true);

                    beforeNow.forEach((t, f) -> assertThat(
                            "now=" + now + ", ts=" + t + ", trackerTs=" + tracker.current(),
                            f, willCompleteSuccessfully())
                    );

                    beforeNow.clear();
                }
            }

            return null;
        }, threads, "trackableHybridClockTest");

        updateTracker(tracker, HybridTimestamp.MAX_VALUE, null);

        assertThat(CompletableFuture.allOf(allFutures.toArray(CompletableFuture[]::new)), willCompleteSuccessfully());
    }

    @ParameterizedTest
    @MethodSource("hybridTimestampTrackerGenerator")
    public void testMultithreadedWaitForWithValue(PendingComparableValuesTracker trackerParam) throws Exception {
        HybridClock clock = new HybridClockImpl();

        @SuppressWarnings("unchecked")
        PendingComparableValuesTracker<HybridTimestamp, HybridTimestamp> tracker = trackerParam;

        int threads = Runtime.getRuntime().availableProcessors();

        List<CompletableFuture<HybridTimestamp>> allFutures = Collections.synchronizedList(new ArrayList<>());

        int iterations = 1_000;

        runMultiThreaded(() -> {
            NavigableMap<HybridTimestamp, CompletableFuture<HybridTimestamp>> prevFutures = new TreeMap<>();

            ThreadLocalRandom random = ThreadLocalRandom.current();

            for (int i = 0; i < iterations; i++) {
                HybridTimestamp now = clock.now();

                updateTracker(tracker, now, now);

                HybridTimestamp timestampToWait =
                        new HybridTimestamp(now.getPhysical() + 1, now.getLogical() + random.nextInt(1000));

                CompletableFuture<HybridTimestamp> future = tracker.waitFor(timestampToWait);

                prevFutures.put(timestampToWait, future);

                allFutures.add(future);

                if (i % 10 == 0) {
                    SortedMap<HybridTimestamp, CompletableFuture<HybridTimestamp>> beforeNow = prevFutures.headMap(now, true);

                    beforeNow.forEach((t, f) -> assertThat(
                            "now=" + now + ", ts=" + t + ", trackerTs=" + tracker.current(),
                            f, willBe(greaterThanOrEqualTo(t)))
                    );
                    beforeNow.clear();
                }
            }

            return null;
        }, threads, "trackableHybridClockTest");

        updateTracker(tracker, HybridTimestamp.MAX_VALUE, null);

        assertThat(CompletableFuture.allOf(allFutures.toArray(CompletableFuture[]::new)), willCompleteSuccessfully());
    }

    @RepeatedTest(100)
    void testConcurrentAccess() {
        var tracker = new PendingComparableValuesTracker<>(1);

        var barrier = new CyclicBarrier(2);

        CompletableFuture<Void> writerFuture = CompletableFuture.runAsync(() -> {
            try {
                barrier.await();
                updateTracker(tracker, 2, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        CompletableFuture<Void> readerFuture = CompletableFuture.runAsync(() -> {
            try {
                barrier.await();
                tracker.waitFor(2).get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(writerFuture, willCompleteSuccessfully());
        assertThat(readerFuture, willCompleteSuccessfully());
    }

    @ParameterizedTest
    @MethodSource("hybridTimestampTrackerGenerator")
    void testClose(PendingComparableValuesTracker trackerParam) {
        doTestClose(trackerParam);
    }

    private static Stream<PendingComparableValuesTracker> hybridTimestampTrackerGenerator() {
        HybridTimestamp ts = new HybridTimestamp(1, 0);

        return Stream.of(
                new PendingComparableValuesTracker<>(ts),
                new PendingIndependentComparableValuesTracker<>(ts),
                new SafeTimeValuesTracker(ts)
        );
    }
}
