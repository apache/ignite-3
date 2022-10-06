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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class OrderedCompletableFutureTest {
    /**
     * Tests ordered completion of future callbacks.
     */
    @Test
    void correctOrderWhenComplectingNormaly() throws Exception {
        CompletableFuture<String> future = new OrderedCompletableFuture<>();

        testOrderingForNormalCompletion(future, future);
    }

    /**
     * Tests that callbacks added on a future gets executed orderly when the root future is completed.
     *
     * @param future Future with which to operate (add callbacks and check completion). This is a dependent of the root future.
     * @param root   Future that, when completed, will cause completion of the first future.
     * @throws Exception If something goes wrong.
     */
    private static void testOrderingForNormalCompletion(CompletableFuture<String> future, CompletableFuture<String> root) throws Exception {
        AtomicInteger order = new AtomicInteger();
        List<CompletableFuture<?>> dependents = new ArrayList<>();

        dependents.add(future.thenRun(() -> assertEquals(1, order.incrementAndGet())));
        dependents.add(future.thenAccept(r -> {
            assertEquals(2, order.incrementAndGet());
            assertEquals("foo", r);
        }));
        dependents.add(future.thenApply(r -> {
            assertEquals(3, order.incrementAndGet());
            assertEquals("foo", r);
            return "bar";
        }));
        dependents.add(future.thenCompose(r -> {
            assertEquals(4, order.incrementAndGet());
            assertEquals("foo", r);
            return CompletableFuture.completedFuture("bar");
        }));
        dependents.add(future.thenCombine(CompletableFuture.completedFuture(null), (r1, r2) -> {
            assertEquals(5, order.incrementAndGet());
            assertEquals("foo", r1);
            return CompletableFuture.completedFuture("bar");
        }));
        dependents.add(future.handle((r, e) -> {
            assertEquals(6, order.incrementAndGet());
            assertEquals("foo", r);
            return "bar";
        }));
        dependents.add(future.whenComplete((r, e) -> {
            assertEquals(7, order.incrementAndGet());
            assertEquals("foo", r);
        }));

        root.complete("foo");

        future.get();
        CompletableFuture.allOf(dependents.toArray(CompletableFuture[]::new)).get();
    }

    /**
     * Tests ordered completion of future callbacks on a dependent future.
     */
    @Test
    void correctOrderOnDependentWhenComplectingNormaly() throws Exception {
        CompletableFuture<String> root = new OrderedCompletableFuture<>();
        CompletableFuture<String> dependent = root.thenCompose(CompletableFuture::completedFuture);

        testOrderingForNormalCompletion(dependent, root);
    }

    /**
     * Tests ordered failure of future callbacks.
     */
    @Test
    void correctOrderWhenCompletingExceptionally() {
        CompletableFuture<String> future = new OrderedCompletableFuture<>();
        List<CompletableFuture<?>> dependents = new ArrayList<>();
        List<Integer> orders = new CopyOnWriteArrayList<>();

        dependents.add(future.thenRun(() -> orders.add(1)));
        dependents.add(future.thenAccept(r -> orders.add(2)));
        dependents.add(future.thenApply(r -> orders.add(3)));
        dependents.add(future.thenCompose(r -> CompletableFuture.completedFuture(orders.add(4))));
        dependents.add(future.thenCombine(
                CompletableFuture.completedFuture(null),
                (r1, r2) -> CompletableFuture.completedFuture(orders.add(5)))
        );
        dependents.add(future.handle((r, e) -> orders.add(6)));
        dependents.add(future.whenComplete((r, e) -> orders.add(7)));

        future.completeExceptionally(new RuntimeException("foo"));

        assertThrows(ExecutionException.class, () -> CompletableFuture.allOf(dependents.toArray(CompletableFuture[]::new)).get());

        assertThat(orders, contains(6, 7));
    }

    /**
     * Tests calling callbacks that are added after completion.
     */
    @Test
    void correctOrderWhenAddingAfterCompletion() {
        CompletableFuture<String> future = new OrderedCompletableFuture<>();

        future.whenComplete((result, error) -> assertEquals(result, "foo"));
        future.complete("foo");
        AtomicInteger count = new AtomicInteger();
        future.whenComplete((result, error) -> {
            assertEquals(result, "foo");
            assertEquals(count.incrementAndGet(), 1);
        });
        future.thenRun(() -> assertEquals(count.incrementAndGet(), 2));
        future.thenAccept(result -> {
            assertEquals(result, "foo");
            assertEquals(count.incrementAndGet(), 3);
        });
        future.thenCompose(result -> {
            assertEquals(result, "foo");
            assertEquals(count.incrementAndGet(), 4);
            return CompletableFuture.completedFuture(null);
        });

        assertEquals(count.get(), 4);
    }

    @Test
    void descendantsAreOrdered() {
        CompletableFuture<?> root = new OrderedCompletableFuture<>();

        assertThat(root.thenRun(() -> {}), is(instanceOf(OrderedCompletableFuture.class)));
        assertThat(root.thenAccept(x -> {}), is(instanceOf(OrderedCompletableFuture.class)));
        assertThat(root.thenApply(x -> x), is(instanceOf(OrderedCompletableFuture.class)));
        assertThat(root.thenCompose(x -> CompletableFuture.completedFuture(null)), is(instanceOf(OrderedCompletableFuture.class)));
        assertThat(
                root.thenCombine(CompletableFuture.completedFuture(null), (x, y) -> CompletableFuture.completedFuture(null)),
                is(instanceOf(OrderedCompletableFuture.class))
        );
    }
}
