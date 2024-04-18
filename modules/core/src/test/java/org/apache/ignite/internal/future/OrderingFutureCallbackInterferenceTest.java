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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * Tests making sure that, when one callback throws an exception, its fellows still get a chance to be completed.
 */
class OrderingFutureCallbackInterferenceTest {
    private final RuntimeException cause = new RuntimeException("Oops");

    @Test
    void composeToCompletableDoesNotInterfereWithEachOther() {
        OrderingFuture<Integer> future = new OrderingFuture<>();

        List<Integer> order = new CopyOnWriteArrayList<>();

        future.thenComposeToCompletable(x -> {
            throw cause;
        });
        future.thenComposeToCompletable(x -> {
            order.add(1);
            return nullCompletedFuture();
        });
        future.thenComposeToCompletable(x -> {
            throw cause;
        });
        future.thenComposeToCompletable(x -> {
            order.add(2);
            return nullCompletedFuture();
        });

        future.complete(1);

        assertThat(order, contains(1, 2));
    }

    @Test
    void composeDoesNotInterfereWithEachOther() {
        OrderingFuture<Integer> future = new OrderingFuture<>();

        List<Integer> order = new CopyOnWriteArrayList<>();

        future.thenCompose(x -> {
            throw cause;
        });
        future.thenCompose(x -> {
            order.add(1);
            return OrderingFuture.completedFuture(null);
        });
        future.thenCompose(x -> {
            throw cause;
        });
        future.thenCompose(x -> {
            order.add(2);
            return OrderingFuture.completedFuture(null);
        });

        future.complete(1);

        assertThat(order, contains(1, 2));
    }

    @Test
    void whenCompleteDoesNotInterfereWithEachOther() {
        OrderingFuture<Integer> future = new OrderingFuture<>();

        List<Integer> order = new CopyOnWriteArrayList<>();

        future.whenComplete((res, ex) -> {
            throw new RuntimeException("one");
        });
        future.whenComplete((res, ex) -> order.add(1));
        future.whenComplete((res, ex) -> {
            throw new RuntimeException("three");
        });
        future.whenComplete((res, ex) -> order.add(2));

        future.complete(1);

        assertThat(order, contains(1, 2));
    }
}
