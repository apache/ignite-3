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

package org.apache.ignite.internal.future;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests for concurrency aspects of {@link OrderingFuture}.
 */
class OrderingFutureConcurrencyTest {
    @Test
    void concurrentAdditionOfCallbacksIsCorrect() throws Exception {
        OrderingFuture<Integer> future = new OrderingFuture<>();

        AtomicInteger counter = new AtomicInteger();

        Runnable addIncrementerTask = () -> {
            for (int i = 0; i < 10_000; i++) {
                future.thenComposeToCompletable(x -> {
                    counter.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });
            }
        };

        Thread thread1 = new Thread(addIncrementerTask);
        Thread thread2 = new Thread(addIncrementerTask);

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        future.complete(1);

        assertThat(counter.get(), is(20_000));
    }
}
