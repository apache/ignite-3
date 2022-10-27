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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Composite publisher test.
 */
public class CompositePublisherTest {
    @Test
    public void testEnoughData() throws InterruptedException {
        doTestPublisher(1, 2, 2, true, false);

        doTestPublisher(50, 357, 7, true, false);
        doTestPublisher(50, 357, 7, false, false);
    }

    @Test
    public void testNotEnoughData() throws InterruptedException {
        doTestPublisher(1, 0, 2, true, false);

        doTestPublisher(100, 70, 7, true, false);
        doTestPublisher(100, 70, 7, false, false);
    }

    @Test
    public void testMultipleRequest() throws InterruptedException {
        doTestPublisher(100, 70, 7, true, true);
        doTestPublisher(100, 70, 7, false, true);
    }

    @Test
    public void testExactEnoughData() throws InterruptedException {
        doTestPublisher(30, 30, 3, true, false);
        doTestPublisher(30, 30, 3, false, false);
    }

    private void doTestPublisher(int requestCnt, int totalCnt, int threadCnt, boolean random, boolean split) throws InterruptedException {
        int dataCnt = totalCnt / threadCnt;
        Integer[][] data = new Integer[threadCnt][dataCnt];
        int[] expData = new int[totalCnt];
        int k = 0;

        for (int i = 0; i < threadCnt; i++) {
            for (int j = 0; j < dataCnt; j++) {
                data[i][j] = random ? ThreadLocalRandom.current().nextInt(totalCnt) : k;

                expData[k++] = data[i][j];
            }

            Arrays.sort(data[i]);
        }

        Arrays.sort(expData);

        List<TestPublisher<Integer>> publishers = new ArrayList<>();

        for (int i = 0; i < threadCnt; i++) {
            TestPublisher<Integer> pub = new TestPublisher<>(data[i]);

            publishers.add(pub);
        }

        AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
        SubscriberListener<Integer> lsnr = new SubscriberListener<>();

        new CompositePublisher<>(publishers, Comparator.comparingInt(v -> v)).subscribe(new Subscriber<>() {
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
                    t.printStackTrace();
                }

                @Override
                public void onComplete() {
                    lsnr.onComplete();
                }
        });

        if (!split) {
            checkSubscriptionRequest(subscriptionRef.get(), new InputParameters(expData, requestCnt), lsnr);
        } else {
            InputParameters params = new InputParameters(expData, 1);

            for (int off = 0; off < Math.min(requestCnt, totalCnt); off++) {
                checkSubscriptionRequest(subscriptionRef.get(), params.offset(off), lsnr);
            }
        }

        // after test
        for (TestPublisher<Integer> pub : publishers) {
            pub.waitSuppliersTermination();
        }
    }

    private void checkSubscriptionRequest(Subscription subscription, InputParameters params, SubscriberListener<Integer> lsnr)
            throws InterruptedException {
        lsnr.reset(params.requested);

        subscription.request(params.requested);

        Assertions.assertTrue(lsnr.awaitComplete(10), "Execution timeout");

        int remaining = params.total - params.offset;
        int expReceived = Math.min(params.requested, remaining);
        int[] expResult = Arrays.copyOfRange(params.data, params.offset, params.offset + expReceived);

        Assertions.assertEquals(expReceived, lsnr.res.size());
        Assertions.assertEquals(expReceived, lsnr.receivedCnt.get());

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
        private final Queue<CompletableFuture<?>> futs = new LinkedList<>();
        private final AtomicBoolean completed = new AtomicBoolean();
        private Subscriber<? super T> subscriber;

        TestPublisher(T[] data) {
            this.data = data;
        }

        void waitSuppliersTermination() {
            CompletableFuture<?> fut;

            while ((fut = futs.poll()) != null) {
                try {
                    fut.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Supplier future completed with an error,", e);
                }
            }
        }

        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            this.subscriber = subscriber;

            subscriber.onSubscribe(new TestSubscription());
        }

        class TestSubscription implements Subscription {
            AtomicInteger idx = new AtomicInteger(0);

            @Override
            public void request(long n) {
                CompletableFuture<Long> fut = CompletableFuture.supplyAsync(() -> {
                    int startIdx = idx.getAndAdd((int) n);
                    int endIdx = Math.min(startIdx + (int) n, data.length);

                    for (int n0 = startIdx; n0 < endIdx; n0++) {
                        subscriber.onNext(data[n0]);
                    }

                    if (endIdx >= data.length && completed.compareAndSet(false, true)) {
                        subscriber.onComplete();
                    }

                    return n;
                });

                futs.add(fut);
            }

            @Override
            public void cancel() {
                subscriber.onError(new RuntimeException("cancelled"));
            }
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
        AtomicInteger receivedCnt = new AtomicInteger();
        AtomicInteger onCompleteCntr = new AtomicInteger();
        Collection<T> res = new LinkedBlockingQueue<>();
        volatile CountDownLatch waitLatch = new CountDownLatch(1);
        AtomicReference<Integer> requestedCnt = new AtomicReference<>();

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
