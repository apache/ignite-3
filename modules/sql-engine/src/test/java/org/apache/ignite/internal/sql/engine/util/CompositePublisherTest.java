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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.sql.engine.exec.comp.CompositePublisher;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

    class InputParameters {
        int[] data;
        int offset;
        int total;
        int requested;

        InputParameters(int[] data, int requested) {
            this.data = data;
            this.requested = requested;
            this.total = data.length;
        }

        public InputParameters offset(int offset) {
            this.offset = offset;

            return this;
        }
    }

    class ValidationData {

    }

    public void doTestPublisher(int requestCnt, int totalCnt, int threadCnt, boolean random, boolean split) throws InterruptedException {
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

        LinkedBlockingQueue<Integer> res = new LinkedBlockingQueue<>();
        CompositePublisher<Integer> publisher = new CompositePublisher<>(Comparator.comparingInt(v -> v));

        List<TestPublisher<Integer>> publishers = new ArrayList<>();

        for (int i = 0; i < threadCnt; i++) {
            TestPublisher<Integer> pub = new TestPublisher<>(data[i]);

            publishers.add(pub);
            publisher.add(pub);
        }

        AtomicReference<CountDownLatch> finishLatchRef = new AtomicReference<>();
        AtomicInteger receivedCnt = new AtomicInteger();
        AtomicInteger onCompleteCntr = new AtomicInteger();
        AtomicReference<Subscription> subscriptionRef =new AtomicReference<>();
        AtomicReference<Integer> requestedCnt = new AtomicReference<>();

        publisher.subscribe(new Subscriber<>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscriptionRef.set(subscription);
                }

                @Override
                public void onNext(Integer item) {
                    debug(">[xxx]> " + item);

                    res.add(item);

                    if (receivedCnt.incrementAndGet() == requestedCnt.get())
                        finishLatchRef.get().countDown();
                }

                @Override
                public void onError(Throwable throwable) {
                    throwable.printStackTrace();
                }

                @Override
                public void onComplete() {
                    debug(">[xxx]> subscription complete");

                    finishLatchRef.get().countDown();
                    onCompleteCntr.incrementAndGet();
                }
        });

        if (!split) {
            InputParameters params = new InputParameters(expData, requestCnt);

            checkSubscriptionRequest(params, requestedCnt, subscriptionRef.get(), res, receivedCnt, onCompleteCntr, finishLatchRef);
        }
         else {
            debug("Initial data: " + Arrays.toString(expData));

            InputParameters params = new InputParameters(expData, 1);

            for (int off = 0; off < Math.min(requestCnt, totalCnt); off++) {
                params.offset(off);

                checkSubscriptionRequest(params, requestedCnt, subscriptionRef.get(), res, receivedCnt, onCompleteCntr, finishLatchRef);
            }
        }

        // after test
        for (TestPublisher<Integer> pub : publishers) {
            pub.waitComplete();
        }
    }

    private void checkSubscriptionRequest(
            InputParameters params,
            AtomicReference<Integer> requestedCnt,
            Subscription subscription,
            Collection<Integer> res,
            AtomicInteger receivedCnt,
            AtomicInteger onCompleteCntr,
            AtomicReference<CountDownLatch> finishLatchRef
    ) throws InterruptedException {
        debug(">xxxx> --------------------------------------------------------------------------------");
        debug(">xxxx> request next [off=" + params.offset + ", requested=" + params.requested + ", total=" + params.total + ']');
        debug(">xxxx> --------------------------------------------------------------------------------");
        receivedCnt.set(0);
        finishLatchRef.set(new CountDownLatch(1));
        requestedCnt.set(params.requested);
        res.clear();

        subscription.request(params.requested);

        Assertions.assertTrue(finishLatchRef.get().await(10, TimeUnit.SECONDS), "Execution timeout");

        int remaining = params.total - params.offset;
        int expReceived = Math.min(params.requested, remaining);
        int[] expResult = Arrays.copyOfRange(params.data, params.offset, params.offset + expReceived);

        Assertions.assertEquals(expReceived, res.size());
        Assertions.assertEquals(expReceived, receivedCnt.get());

        int expCnt = params.offset + params.requested >= params.total ? 1 : 0;
        IgniteTestUtils.waitForCondition(() -> onCompleteCntr.get() == expCnt, 10_000);

        Assertions.assertEquals(expCnt, onCompleteCntr.get());

        int[] resArr = new int[expReceived];

        int k = 0;

        for (Integer n : res) {
            resArr[k++] = n;
        }

        Assertions.assertArrayEquals(expResult, resArr, "\n" + Arrays.toString(expResult) + "\n" + Arrays.toString(resArr) + "\n");
    }

    private static class TestPublisher<T> implements Publisher<T> {
        private final T[] data;
        Queue<CompletableFuture<?>> futs = new LinkedList<>();

        TestPublisher(T[] data) {
            this.data = data;
        }

        class TestSubscription implements Subscription {
            AtomicInteger idx = new AtomicInteger(0);

            @Override
            public void request(long n) {
                CompletableFuture<Long> fut = CompletableFuture.supplyAsync(() -> {
                    int startIdx = idx.getAndAdd((int) n);
                    int endIdx = Math.min(startIdx + (int) n, data.length);

                    T[] subArr = Arrays.copyOfRange(data, startIdx, endIdx);

                    debug(">xxx> push " + Arrays.toString(subArr) + " subscr=" + subscriber);

                    try {
                        for (int n0 = startIdx; n0 < endIdx; n0++) {
                            subscriber.onNext(data[n0]);
                        }
                    } catch (Throwable t) {
                        System.err.println("-------------");
                        t.printStackTrace();
                        System.err.println("-------------");

                        throw t;
                    }

//                    debug(">xxx> push " + Arrays.toString(subArr) + " END   subscr=" + subscriber);

                    if (endIdx >= data.length) {
                        debug(">xxx> onCOmplete " + subscriber);
                        subscriber.onComplete();
                    } else {
                        debug("endIdx=" + endIdx + ", data.length=" + data.length);
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

        public void waitComplete() {
            CompletableFuture<?> fut = futs.poll();

            if (fut != null) {
                try {
                    fut.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();

                    throw new RuntimeException(e);
                }
            }
        }

        private Subscriber<? super T> subscriber;

        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            this.subscriber = subscriber;

            subscriber.onSubscribe(new TestSubscription());
        }
    }

    private static boolean debug = false;

    private static void debug(String msg) {
        if (debug)
            System.out.println(msg);
    }
}
