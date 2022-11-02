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

package org.apache.ignite.internal.sql.engine.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Composite subscription test.
 */
public class CompositeSubscriptionTest {
    @Test
    public void testEnoughData() throws Throwable {
        doPublishSubscribe(1, 2, 2, true, false, true);
        doPublishSubscribe(1, 2, 2, true, false, false);

        doPublishSubscribe(50, 357, 7, true, false, true);
        doPublishSubscribe(50, 357, 7, true, false, false);

        doPublishSubscribe(50, 357, 7, false, false, true);
    }

    @Test
    public void testNotEnoughData() throws Throwable {
        doPublishSubscribe(1, 0, 2, true, false, true);
        doPublishSubscribe(1, 0, 2, true, false, false);

        doPublishSubscribe(100, 70, 7, true, false, true);
        doPublishSubscribe(100, 70, 7, true, false, false);

        doPublishSubscribe(100, 70, 7, false, false, true);
    }

    @Test
    public void testMultipleRequest() throws Throwable {
        doPublishSubscribe(100, 70, 7, true, true, true);
        doPublishSubscribe(100, 70, 7, true, true, false);

        doPublishSubscribe(100, 70, 7, false, true, true);
    }

    @Test
    public void testExactEnoughData() throws Throwable {
        doPublishSubscribe(30, 30, 3, true, false, true);
        doPublishSubscribe(30, 30, 3, true, false, false);

        doPublishSubscribe(30, 30, 3, false, false, true);
    }

    /**
     * Test composite publishing-subscribing.
     *
     * @param cnt Number of requested items.
     * @param total Total number of available items.
     * @param pubCnt Number of publishers.
     * @param rnd {@code True} to generate random data, otherwise sequential range will be generated.
     * @param split Indicates that we will divide the requested amount into small sub-requests.
     * @param sort {@code True} to test merge sort strategy, otherwise all requests will be sent sequentially in single threaded mode.
     * @throws InterruptedException If failed.
     */
    private static void doPublishSubscribe(int cnt, int total, int pubCnt, boolean rnd, boolean split, boolean sort) throws Throwable {
        int dataCnt = total / pubCnt;
        Integer[][] data = new Integer[pubCnt][dataCnt];
        int[] expData = new int[total];
        int k = 0;

        for (int i = 0; i < pubCnt; i++) {
            for (int j = 0; j < dataCnt; j++) {
                data[i][j] = rnd ? ThreadLocalRandom.current().nextInt(total) : k;

                expData[k++] = data[i][j];
            }

            if (sort) {
                Arrays.sort(data[i]);
            }
        }

        if (sort) {
            Arrays.sort(expData);
        }

        List<TestPublisher<Integer>> publishers = new ArrayList<>();

        for (int i = 0; i < pubCnt; i++) {
            TestPublisher<Integer> pub = new TestPublisher<>(data[i], sort);

            publishers.add(pub);
        }

        AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
        SubscriberListener<Integer> lsnr = new SubscriberListener<>();

        Subscriber<Integer> subscr = new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscriptionRef.set(subscription);
            }

            @Override
            public void onNext(Integer item) {
                lsnr.onNext(item);
            }

            @Override
            public void onError(Throwable t) {
                assert false;
            }

            @Override
            public void onComplete() {
                lsnr.onComplete();
            }
        };

        CompositeSubscription<Integer> compSubscription;

        if (sort) {
            compSubscription = new OrderedMergeCompositeSubscription<>(subscr, Comparator.comparingInt(v -> v),
                    Commons.SORTED_IDX_PART_PREFETCH_SIZE, pubCnt);
        } else {
            compSubscription = new CompositeSubscription<>(subscr);
        }

        lsnr.reset(cnt);

        compSubscription.subscribe(publishers);

        subscr.onSubscribe(compSubscription);

        if (!split) {
            checkSubscriptionRequest(subscriptionRef.get(), new InputParameters(expData, cnt), lsnr);
        } else {
            InputParameters params = new InputParameters(expData, 1);

            for (int off = 0; off < Math.min(cnt, total); off++) {
                lsnr.reset(params.requested);

                checkSubscriptionRequest(subscriptionRef.get(), params.offset(off), lsnr);
            }
        }

        // Check errors.
        for (TestPublisher<Integer> pub : publishers) {
            Throwable err = pub.errRef.get();

            if (err != null) {
                throw err;
            }
        }
    }

    private static void checkSubscriptionRequest(Subscription subscription, InputParameters params, SubscriberListener<Integer> lsnr)
            throws InterruptedException {
        subscription.request(params.requested);

        Assertions.assertTrue(lsnr.awaitComplete(10), "Execution timeout");

        int remaining = params.total - params.offset;
        int expReceived = Math.min(params.requested, remaining);
        int[] expResult = Arrays.copyOfRange(params.data, params.offset, params.offset + expReceived);

        Assertions.assertEquals(expReceived, lsnr.res.size());
        Assertions.assertEquals(expReceived, lsnr.receivedCnt.get());

        // Ensures that onComplete has (not) been called.
        int expCnt = params.offset + params.requested >= params.total ? 1 : 0;
        IgniteTestUtils.waitForCondition(() -> lsnr.onCompleteCntr.get() == expCnt, 10_000);
        Assertions.assertEquals(expCnt, lsnr.onCompleteCntr.get());

        int[] resArr = new int[expReceived];
        int k = 0;

        for (Integer n : lsnr.res) {
            resArr[k++] = n;
        }

        Assertions.assertArrayEquals(expResult, resArr, "\n" + Arrays.toString(expResult) + "\n" + Arrays.toString(resArr) + "\n");
    }

    private static class TestPublisher<T> implements Publisher<T> {
        private final T[] data;
        private final AtomicBoolean publicationComplete = new AtomicBoolean();
        private final AtomicReference<Throwable> errRef = new AtomicReference<>();
        private final boolean async;

        TestPublisher(T[] data, boolean async) {
            this.data = data;
            this.async = async;
        }

        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            subscriber.onSubscribe(new Subscription() {
                private final AtomicInteger idx = new AtomicInteger(0);

                @SuppressWarnings("NumericCastThatLosesPrecision")
                @Override
                public void request(long n) {
                    Supplier<Long> dataSupplier = () -> {
                        int startIdx = idx.getAndAdd((int) n);
                        int endIdx = Math.min(startIdx + (int) n, data.length);

                        for (int n0 = startIdx; n0 < endIdx; n0++) {
                            subscriber.onNext(data[n0]);
                        }

                        if (endIdx >= data.length && publicationComplete.compareAndSet(false, true)) {
                            subscriber.onComplete();
                        }

                        return n;
                    };

                    if (!async) {
                        try {
                            dataSupplier.get();
                        } catch (Throwable t) {
                            handleError(t);
                        }

                        return;
                    }

                    CompletableFuture.supplyAsync(dataSupplier)
                            .whenComplete((res, err) -> {
                                if (err != null) {
                                    handleError(err);
                                }
                            });
                }

                @Override
                public void cancel() {
                    throw new UnsupportedOperationException();
                }
            });
        }

        @SuppressWarnings("CallToPrintStackTrace")
        private void handleError(Throwable err) {
            err.printStackTrace();

            errRef.compareAndSet(null, err);
        }
    }

    private static class InputParameters {
        int[] data;
        int offset;
        int total;
        int requested;

        InputParameters(int[] data, int requested) {
            this.data = data;
            this.requested = requested;
            this.total = data.length;
        }

        InputParameters offset(int offset) {
            this.offset = offset;

            return this;
        }
    }

    private static class SubscriberListener<T> {
        final AtomicInteger receivedCnt = new AtomicInteger();
        final AtomicInteger onCompleteCntr = new AtomicInteger();
        final Collection<T> res = new LinkedBlockingQueue<>();
        final AtomicReference<Integer> requestedCnt = new AtomicReference<>();
        volatile CountDownLatch waitLatch;

        void reset(int requested) {
            receivedCnt.set(0);
            waitLatch = new CountDownLatch(1);
            requestedCnt.set(requested);
            res.clear();
        }

        void onNext(T item) {
            res.add(item);

            if (receivedCnt.incrementAndGet() == requestedCnt.get()) {
                waitLatch.countDown();
            }
        }

        boolean awaitComplete(int timeout) throws InterruptedException {
            return waitLatch.await(timeout, TimeUnit.SECONDS);
        }

        void onComplete() {
            waitLatch.countDown();
            onCompleteCntr.incrementAndGet();
        }
    }
}
