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

package org.apache.ignite.internal.causality;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.causality.IncrementalVersionedValue.dependingOn;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link IncrementalVersionedValue}.
 */
public class IncrementalVersionedValueTest extends BaseIgniteAbstractTest {
    /** Test value. */
    private static final int TEST_VALUE = 1;

    /** The test revision register is used to move the revision forward. */
    private final TestRevisionRegister register = new TestRevisionRegister();

    /** Test exception is used for exceptionally completion Versioned value object. */
    private static final Exception TEST_EXCEPTION = new Exception("Test exception");

    /**
     * Checks that the update method work as expected when the previous value is known.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdate() throws Exception {
        var versionedValue = new IncrementalVersionedValue<Integer>(register);

        versionedValue.update(0, (integer, throwable) -> completedFuture(TEST_VALUE));

        register.moveRevision(0L).join();

        CompletableFuture<Integer> fut = versionedValue.get(1);

        assertFalse(fut.isDone());

        int incrementCount = 10;

        for (int i = 0; i < incrementCount; i++) {
            versionedValue.update(1, (previous, e) -> completedFuture(++previous));

            assertFalse(fut.isDone());
        }

        register.moveRevision(1L).join();

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE + incrementCount, fut.get());

        assertThrows(AssertionError.class, () -> versionedValue.update(1L, (i, t) -> nullCompletedFuture()));
    }

    /**
     * Checks that the update method work as expected when there is no history to calculate previous value.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdatePredefined() throws Exception {
        var versionedValue = new IncrementalVersionedValue<Integer>(register);

        CompletableFuture<Integer> fut = versionedValue.get(0);

        assertFalse(fut.isDone());

        versionedValue.update(0, (previous, e) -> {
            assertNull(previous);

            return completedFuture(TEST_VALUE);
        });

        assertFalse(fut.isDone());

        register.moveRevision(0L).join();

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE, fut.get());
    }

    /**
     * Test asynchronous update closure.
     */
    @Test
    public void testAsyncUpdate() {
        IncrementalVersionedValue<Integer> vv = new IncrementalVersionedValue<>(register);

        CompletableFuture<Integer> fut = new CompletableFuture<>();

        vv.update(0L, (v, e) -> fut);

        CompletableFuture<Integer> vvFut = vv.get(0L);

        CompletableFuture<?> revFut = register.moveRevision(0L);

        assertFalse(fut.isDone());
        assertFalse(vvFut.isDone());
        assertFalse(revFut.isDone());

        fut.complete(1);

        revFut.join();

        assertTrue(vvFut.isDone());
    }

    /**
     * Test the case when exception happens in updater.
     */
    @Test
    public void testExceptionOnUpdate() {
        IncrementalVersionedValue<Integer> vv = new IncrementalVersionedValue<>(register, () -> 0);

        final int count = 4;
        final int successfulCompletionsCount = count / 2;

        AtomicInteger actualSuccessfulCompletionsCount = new AtomicInteger();

        final String exceptionMsg = "test msg";

        for (int i = 0; i < count; i++) {
            vv.update(0L, (v, e) -> {
                if (e != null) {
                    return failedFuture(e);
                }

                if (v == successfulCompletionsCount) {
                    throw new IgniteInternalException(exceptionMsg);
                }

                actualSuccessfulCompletionsCount.incrementAndGet();

                return completedFuture(++v);
            });
        }

        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();

        vv.whenComplete((t, v, e) -> exceptionRef.set(e));

        vv.complete(0L);

        assertThrowsWithCause(() -> vv.get(0L).join(), IgniteInternalException.class);

        assertThat(exceptionRef.get().getMessage(), containsString(exceptionMsg));
    }

    /**
     * Test with multiple versioned values and asynchronous completion.
     */
    @Test
    public void testAsyncMultiVv() {
        final String registryName = "Registry";
        final String assignmentName = "Assignment";
        final String tableName = "T1_";

        IncrementalVersionedValue<Map<UUID, String>> tablesVv = new IncrementalVersionedValue<>(f -> {
        }, HashMap::new);
        IncrementalVersionedValue<Map<UUID, String>> schemasVv = new IncrementalVersionedValue<>(register, HashMap::new);
        IncrementalVersionedValue<Map<UUID, String>> assignmentsVv = new IncrementalVersionedValue<>(register, HashMap::new);

        schemasVv.whenComplete((token, value, ex) -> tablesVv.complete(token));

        BiFunction<Long, UUID, CompletableFuture<String>> schemaRegistry =
                (token, uuid) -> schemasVv.get(token).thenApply(schemas -> schemas.get(uuid));

        // Adding table.
        long token = 0L;
        UUID tableId = UUID.randomUUID();

        CompletableFuture<String> tableFut = schemaRegistry.apply(token, tableId)
                .thenCombine(assignmentsVv.get(token), (registry, assignments) -> tableName + registry + assignments.get(tableId));

        tablesVv.update(token, (old, e) -> tableFut.thenApply(table -> {
            Map<UUID, String> val = new HashMap<>(old);

            val.put(tableId, table);

            return val;
        }));

        CompletableFuture<String> userFut = tablesVv.get(token).thenApply(map -> map.get(tableId));

        schemasVv.update(token, (old, e) -> {
            old.put(tableId, registryName);

            return completedFuture(old);
        });

        assignmentsVv.update(token, (old, e) -> {
            old.put(tableId, assignmentName);

            return completedFuture(old);
        });

        assertFalse(tableFut.isDone());
        assertFalse(userFut.isDone());

        register.moveRevision(token).join();

        tableFut.join();

        assertEquals(tableName + registryName + assignmentName, userFut.join());
    }

    /**
     * Tests a default value supplier.
     */
    @Test
    public void testDefaultValueSupplier() {
        IncrementalVersionedValue<Integer> vv = new IncrementalVersionedValue<>(register, () -> TEST_VALUE);

        checkDefaultValue(vv, TEST_VALUE);
    }

    /**
     * Tests a case when there is no default value supplier.
     */
    @Test
    public void testWithoutDefaultValue() {
        IncrementalVersionedValue<Integer> vv = new IncrementalVersionedValue<>(register);

        checkDefaultValue(vv, null);
    }

    @RepeatedTest(100)
    void testConcurrentGetAndComplete() {
        var versionedValue = new IncrementalVersionedValue<>(register, () -> 1);

        // Set initial value.
        versionedValue.complete(1);

        runRace(
                () -> versionedValue.complete(3),
                () -> {
                    CompletableFuture<Integer> readerFuture = versionedValue.get(2);

                    assertThat(readerFuture, willBe(1));
                }
        );
    }

    @RepeatedTest(100)
    void testConcurrentGetAndCompleteWithHistoryTrimming() {
        var versionedValue = new IncrementalVersionedValue<>(register, 2, () -> 1);

        // Set initial value (history size 1).
        versionedValue.complete(2);
        // Set history size to 2.
        versionedValue.complete(3);

        runRace(
                () -> {
                    versionedValue.update(4, (i, t) -> completedFuture(i + 1));
                    // Trigger history trimming
                    versionedValue.complete(4);
                },
                () -> {
                    try {
                        CompletableFuture<Integer> readerFuture = versionedValue.get(2);

                        assertThat(readerFuture, willBe(1));
                    } catch (OutdatedTokenException ignored) {
                        // This is considered as a valid outcome.
                    }
                }
        );

        // Check that history has indeed been trimmed.
        assertThrows(OutdatedTokenException.class, () -> versionedValue.get(2));

        assertThat(versionedValue.get(4), willBe(2));
    }

    @Test
    void testCompleteMultipleFutures() {
        var versionedValue = new IncrementalVersionedValue<>(register, () -> 1);

        // Set initial value.
        versionedValue.complete(1);

        CompletableFuture<Integer> future1 = versionedValue.get(2);
        CompletableFuture<Integer> future2 = versionedValue.get(3);
        CompletableFuture<Integer> future3 = versionedValue.get(4);

        versionedValue.update(4, (i, t) -> completedFuture(i + 1));

        versionedValue.complete(4);

        assertThat(future1, willBe(1));
        assertThat(future2, willBe(1));
        assertThat(future3, willBe(2));
    }

    /**
     * Tests that {@link IncrementalVersionedValue#dependingOn(IncrementalVersionedValue)} provides causality between 2 different values.
     */
    @RepeatedTest(100)
    public void testDependingOn() {
        var vv0 = new IncrementalVersionedValue<>(register, () -> 1);

        var vv1 = new IncrementalVersionedValue<>(dependingOn(vv0), () -> 1);

        int token = 1;

        vv0.update(token, (i, e) -> supplyAsync(() -> i + 1));

        vv1.update(token, (i, e) -> supplyAsync(() -> i + 1));

        register.moveRevision(token);

        assertThat(vv1.get(token), willCompleteSuccessfully());

        assertTrue(vv0.get(token).isDone());
    }

    /**
     * Tests that {@link IncrementalVersionedValue#update(long, BiFunction)} closure is called immediately when underlying value is
     * accessible, i.e. when there were no other updates.
     */
    @Test
    public void testImmediateUpdate() {
        var vv = new IncrementalVersionedValue<>(register, () -> 1);

        //noinspection unchecked
        BiFunction<Integer, Throwable, CompletableFuture<Integer>> closure = mock(BiFunction.class);

        when(closure.apply(any(), any())).thenReturn(nullCompletedFuture());

        int token = 0;

        vv.update(token, closure);

        verify(closure).apply(eq(1), eq(null));

        assertFalse(vv.get(token).isDone());
    }

    /**
     * Test {@link IncrementalVersionedValue#whenComplete}.
     */
    @Test
    public void testWhenComplete() {
        var vv = new IncrementalVersionedValue<>(register, () -> 1);

        CompletionListener<Integer> listener = mock(CompletionListener.class);

        vv.whenComplete(listener);

        // Test complete.
        long token = 0;

        vv.complete(token);

        verify(listener).whenComplete(token, 1, null);

        // Test update.
        token = 1;

        vv.update(token, (i, t) -> completedFuture(i + 1));
        vv.update(token, (i, t) -> completedFuture(i + 1));

        vv.complete(token);

        verify(listener).whenComplete(token, 3, null);

        // Test complete exceptionally.
        token = 2;

        vv.completeExceptionally(token, TEST_EXCEPTION);

        verify(listener).whenComplete(token, null, TEST_EXCEPTION);

        // Test remove listener.
        token = 3;

        vv.removeWhenComplete(listener);

        clearInvocations(listener);

        vv.complete(token);

        verify(listener, never()).whenComplete(anyLong(), any(), any());
    }

    @Test
    public void testLatest() {
        IncrementalVersionedValue<Integer> vv = new IncrementalVersionedValue<>(register);

        // Default token.
        assertEquals(-1, vv.latestCausalityToken());

        vv.update(1, (val, e) -> completedFuture(10));

        // Revision is not yet updated, we still have the old value.
        assertNull(vv.latest());
        assertEquals(-1, vv.latestCausalityToken());

        register.moveRevision(1);

        // Revision is updated.
        assertEquals(10, vv.latest());
        assertEquals(1, vv.latestCausalityToken());

        register.moveRevision(2);

        // Revision is updated second time. Token must be new, value must be the same.
        assertEquals(10, vv.latest());
        assertEquals(2, vv.latestCausalityToken());

        CompletableFuture<Integer> fut = new CompletableFuture<>();
        vv.update(5, (val, e) -> fut);

        register.moveRevision(5);

        // Future is not yet completed, token and value are still the same.
        assertEquals(10, vv.latest());
        assertEquals(2, vv.latestCausalityToken());

        // All handlers must be invoked by te same thread, VV must be completed right after.
        fut.complete(50);

        // Finally, updated value and token.
        assertEquals(50, vv.latest());
        assertEquals(5, vv.latestCausalityToken());
    }

    @Test
    public void testLatestSeveralVersions() {
        IncrementalVersionedValue<Integer> vv = new IncrementalVersionedValue<>(register);

        CompletableFuture<Integer> fut = new CompletableFuture<>();

        vv.update(1, (val, e) -> fut);
        register.moveRevision(1);

        // Value and token are not updated until the future is completed.
        assertNull(vv.latest());
        assertEquals(-1, vv.latestCausalityToken());

        vv.update(5, (val, e) -> completedFuture(50));
        register.moveRevision(5);

        // Value and token are not updated until the future is completed.
        assertNull(vv.latest());
        assertEquals(-1, vv.latestCausalityToken());

        // All handlers must be invoked by te same thread, VV must be completed right after.
        fut.complete(10);

        assertEquals(50, vv.latest());
        assertEquals(5, vv.latestCausalityToken());
    }

    /**
     * Tests a case when there is no default value supplier.
     */
    private void checkDefaultValue(IncrementalVersionedValue<Integer> vv, @Nullable Integer expectedDefault) {
        assertEquals(expectedDefault, vv.latest());

        vv.update(0, (a, e) -> {
                    assertEquals(expectedDefault, vv.latest());

                    return completedFuture(a == null ? null : a + 1);
                }
        );

        assertEquals(expectedDefault, vv.latest());

        CompletableFuture<Integer> f = vv.get(0);

        assertFalse(f.isDone());

        vv.update(0, (a, e) -> completedFuture(a == null ? null : a + 1));

        register.moveRevision(0L).join();

        assertTrue(f.isDone());

        assertEquals(expectedDefault == null ? null : expectedDefault + 2, f.join());
    }

    /**
     * Test revision register.
     */
    private static class TestRevisionRegister implements Consumer<LongFunction<CompletableFuture<?>>> {
        /** Revision consumer. */
        private final List<LongFunction<CompletableFuture<?>>> moveRevisionList = new ArrayList<>();

        @Override
        public void accept(LongFunction<CompletableFuture<?>> function) {
            moveRevisionList.add(function);
        }

        /**
         * Move revision.
         *
         * @param revision Revision.
         * @return Future for all listeners.
         */
        CompletableFuture<?> moveRevision(long revision) {
            CompletableFuture<?>[] futures = moveRevisionList.stream()
                    .map(m -> m.apply(revision))
                    .toArray(CompletableFuture[]::new);

            return CompletableFuture.allOf(futures);
        }
    }
}
