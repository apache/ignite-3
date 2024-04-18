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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Composite subscription test.
 */
public class CompositeSubscriptionTest {
    /** Test case probe count. */
    private static final int PROBE_CNT = 10;

    @Test
    public void testEnoughData() throws Throwable {
        for (int req = 1; req <= PROBE_CNT; req++) {
            doPublishSubscribe(req, req + 1, req + 1, true, false, true);
            doPublishSubscribe(req, req + 1, req + 1, true, false, false);

            for (int pubCnt = 1; pubCnt <= PROBE_CNT * 10; pubCnt += 10) {
                doPublishSubscribe(req, req * pubCnt, pubCnt, true, false, true);
                doPublishSubscribe(req, req * pubCnt, pubCnt, true, false, false);
                doPublishSubscribe(req, req * pubCnt, pubCnt, false, false, true);
            }
        }
    }

    @Test
    public void testNotEnoughData() throws Throwable {
        for (int req = 1; req <= PROBE_CNT; req++) {
            doPublishSubscribe(req, 0, req, true, false, true);
            doPublishSubscribe(req, 0, req, true, false, false);

            for (int pubCnt = 1; pubCnt <= PROBE_CNT * 10; pubCnt += 10) {
                int total = req * pubCnt;
                int requested = total * 2;

                doPublishSubscribe(requested, total, pubCnt, true, false, true);
                doPublishSubscribe(requested, total, pubCnt, true, false, false);
                doPublishSubscribe(requested, total, pubCnt, false, false, true);
            }
        }
    }

    @Test
    public void testMultipleRequest() throws Throwable {
        for (int req = 1; req <= PROBE_CNT; req++) {
            for (int pubCnt = 1; pubCnt <= PROBE_CNT * 10; pubCnt += 10) {
                int total = req * pubCnt;
                int requested = total * 2;

                doPublishSubscribe(requested, total, pubCnt, true, true, true);
                doPublishSubscribe(requested, total, pubCnt, true, true, false);
                doPublishSubscribe(requested, total, pubCnt, false, true, true);
            }
        }
    }

    @Test
    public void testExactEnoughData() throws Throwable {
        for (int req = 1; req <= PROBE_CNT; req++) {
            for (int pubCnt = 1; pubCnt <= PROBE_CNT * 10; pubCnt += 10) {
                int total = req * pubCnt;

                doPublishSubscribe(total, total, pubCnt, true, false, true);
                doPublishSubscribe(total, total, pubCnt, true, false, false);
                doPublishSubscribe(total, total, pubCnt, false, false, true);
            }
        }
    }

    /**
     * Test composite publishing-subscribing.
     *
     * @param requested Number of requested items.
     * @param total Total number of available items.
     * @param pubCnt Number of publishers.
     * @param rnd {@code True} to generate random data, otherwise sequential range will be generated.
     * @param split Indicates that we will divide the requested amount into small sub-requests.
     * @param sort {@code True} to test merge sort strategy, otherwise all requests will be sent sequentially in single threaded mode.
     * @throws InterruptedException If failed.
     */
    private void doPublishSubscribe(
            int requested,
            int total,
            int pubCnt,
            boolean rnd,
            boolean split,
            boolean sort
    ) throws Throwable {
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

        TestPublisher<Integer>[] publishers = new TestPublisher[pubCnt];

        for (int i = 0; i < pubCnt; i++) {
            TestPublisher<Integer> pub = new TestPublisher<>(data[i]);

            publishers[i] = pub;
        }

        AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
        SubscriberListener<Integer> lsnr = new SubscriberListener<>();

        Publisher<Integer> compPublisher = sort
                ? SubscriptionUtils.orderedMerge(Comparator.naturalOrder(), Math.max(1, requested / pubCnt), publishers)
                : SubscriptionUtils.concat(publishers);

        lsnr.reset(requested);

        compPublisher.subscribe(new Subscriber<>() {
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
        });

        if (!split) {
            checkSubscriptionRequest(subscriptionRef.get(), new InputParameters(expData, requested), lsnr);
        } else {
            for (int off = 1; off < Math.min(requested, total); off += off) {
                InputParameters params = new InputParameters(expData, off);

                checkSubscriptionRequest(subscriptionRef.get(), params.offset(off - 1), lsnr.reset(params.requested));
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

    private void checkSubscriptionRequest(
            Subscription subscription,
            InputParameters params,
            SubscriberListener<Integer> lsnr
    ) throws InterruptedException {
        CompletableFuture.runAsync(() -> subscription.request(params.requested));

        Assertions.assertTrue(lsnr.awaitComplete(10),
                "Execution timeout [params=" + params + ", results=" + lsnr + ']');

        int remaining = params.total - params.offset;
        int expReceived = Math.min(params.requested, remaining);
        int[] expResult = Arrays.copyOfRange(params.data, params.offset, params.offset + expReceived);

        Assertions.assertEquals(expReceived, lsnr.res.size(), "params=" + params);
        Assertions.assertEquals(expReceived, lsnr.receivedCnt.get(), "params=" + params);

        // Ensures that onComplete has (not) been called.
        int expCnt = params.offset + params.requested >= params.total ? 1 : 0;
        IgniteTestUtils.waitForCondition(() -> lsnr.onCompleteCntr.get() == expCnt, 10_000);
        Assertions.assertEquals(expCnt, lsnr.onCompleteCntr.get(), "params=" + params);

        int[] resArr = new int[expReceived];
        int k = 0;

        for (Integer n : lsnr.res) {
            resArr[k++] = n;
        }

        Assertions.assertArrayEquals(expResult, resArr, params + "\n" + Arrays.toString(expResult) + "\n" + Arrays.toString(resArr) + "\n");
    }

    private static class TestPublisher<T> implements Publisher<T> {
        private final T[] data;
        private final AtomicReference<Throwable> errRef = new AtomicReference<>();
        private boolean publicationComplete;

        TestPublisher(T[] data) {
            this.data = data;
        }

        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            CompletableFuture.runAsync(() ->
                    subscriber.onSubscribe(new Subscription() {
                        private int idx;

                        @Override
                        public void request(long requested) {
                            Runnable dataSupplier = () -> {
                                int max = Math.min(data.length, idx + (int) requested);

                                while (idx < max) {
                                    subscriber.onNext(data[idx++]);
                                }

                                if (idx == data.length && !publicationComplete) {
                                    publicationComplete = true;

                                    subscriber.onComplete();
                                }
                            };

                            CompletableFuture.runAsync(() -> {
                                // Multiple requests to the same subscription must not be executed concurrently.
                                synchronized (TestPublisher.this) {
                                    dataSupplier.run();
                                }
                            }).whenComplete((res, err) -> {
                                if (err != null) {
                                    handleError(err);
                                }
                            });
                        }

                        @Override
                        public void cancel() {
                            throw new UnsupportedOperationException();
                        }
                    })
            );
        }

        @SuppressWarnings({"CallToPrintStackTrace", "PMD.AvoidPrintStackTrace"})
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

        @Override
        public String toString() {
            return "InputParameters{"
                    + "requested=" + requested
                    + ", total=" + total
                    + ", offset=" + offset
                    + '}';
        }
    }

    private static class SubscriberListener<T> {
        final AtomicInteger receivedCnt = new AtomicInteger();
        final AtomicInteger onCompleteCntr = new AtomicInteger();
        final Collection<T> res = new LinkedBlockingQueue<>();
        final AtomicReference<Integer> requestedCnt = new AtomicReference<>();
        volatile CountDownLatch waitLatch;

        SubscriberListener<T> reset(int requested) {
            receivedCnt.set(0);
            waitLatch = new CountDownLatch(1);
            requestedCnt.set(requested);
            res.clear();

            return this;
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

        @Override
        public String toString() {
            return "SubscriberListener{"
                    + "receivedCnt=" + receivedCnt.get()
                    + ", onCompleteCntr=" + onCompleteCntr.get()
                    + '}';
        }
    }
}
