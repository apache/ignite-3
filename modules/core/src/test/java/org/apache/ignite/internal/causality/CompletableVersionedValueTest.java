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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Tests of causality token implementation based on versioned value.
 * {@link CompletableVersionedValue}
 */
public class CompletableVersionedValueTest extends BaseIgniteAbstractTest {
    /** Test value. */
    private static final int TEST_VALUE = 1;

    /** Test exception is used for exceptionally completion Versioned value object. */
    private static final Exception TEST_EXCEPTION = new Exception("Test exception");

    /**
     * The test gets a value for {@link CompletableVersionedValue} before the value is calculated.
     *
     * @throws OutdatedTokenException If failed.
     */
    @Test
    public void testGetValueBeforeReady() throws OutdatedTokenException {
        CompletableVersionedValue<Integer> intVersionedValue = new CompletableVersionedValue<>("test");

        CompletableFuture<Integer> fut = intVersionedValue.get(0);

        assertFalse(fut.isDone());

        intVersionedValue.complete(0L, TEST_VALUE);

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE, fut.join());

        assertSame(fut, intVersionedValue.get(0));
    }

    /**
     * Test checks completion of several sequential updates.
     */
    @Test
    public void testManualCompleteSeveralTokens() {
        CompletableVersionedValue<Integer> intVersionedValue = new CompletableVersionedValue<>("test");

        IntStream.range(5, 10).forEach(token -> {
            CompletableFuture<Integer> fut = intVersionedValue.get(token);

            assertFalse(fut.isDone());

            intVersionedValue.complete(token, TEST_VALUE);

            assertTrue(fut.isDone());

            assertEquals(TEST_VALUE, fut.join());

            assertSame(fut, intVersionedValue.get(token));
        });
    }

    /**
     * Test checks exceptionally completion of several sequential updates.
     */
    @Test
    public void testManualExceptionallyCompleteSeveralTokens() {
        CompletableVersionedValue<Integer> intVersionedValue = new CompletableVersionedValue<>("test");

        IntStream.range(5, 10).forEach(token -> {
            CompletableFuture<Integer> fut = intVersionedValue.get(token);

            assertFalse(fut.isDone());

            intVersionedValue.completeExceptionally(token, TEST_EXCEPTION);

            assertTrue(fut.isDone());

            assertThrows(Exception.class, fut::get);

            assertThat(intVersionedValue.get(token), willThrow(TEST_EXCEPTION.getClass()));
        });
    }

    /**
     * The test reads a value with the specific token in which the value should not be updated.
     * The read happens after the revision updated.
     *
     * @throws OutdatedTokenException If failed.
     */
    @Test
    public void testMissValueUpdate() throws OutdatedTokenException {
        CompletableVersionedValue<Integer> longVersionedValue = new CompletableVersionedValue<>("test");

        longVersionedValue.complete(0, TEST_VALUE);

        longVersionedValue.complete(1);

        CompletableFuture<Integer> fut = longVersionedValue.get(1);

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE, fut.join());

        assertSame(fut, longVersionedValue.get(0));
    }

    /**
     * Test checks token history size.
     */
    @Test
    public void testObsoleteToken() {
        CompletableVersionedValue<Integer> versionedValue = new CompletableVersionedValue<>("test", 2);

        versionedValue.complete(0, TEST_VALUE);
        versionedValue.complete(1, TEST_VALUE);
        // Internal map size is now 3: one actual and two old tokens. One old token must be removed.
        versionedValue.complete(2, TEST_VALUE);

        assertThrowsExactly(OutdatedTokenException.class, () -> versionedValue.get(0));
    }

    /**
     * Tests that tokens that are newer then the current token do not contribute to the history size calculation.
     */
    @Test
    public void testNewTokensNotGetRemoved() {
        CompletableVersionedValue<Integer> longVersionedValue = new CompletableVersionedValue<>("test", 2);

        longVersionedValue.complete(0, TEST_VALUE);
        longVersionedValue.complete(1, TEST_VALUE);

        // History size is now 2, try adding a token from the future.
        assertDoesNotThrow(() -> longVersionedValue.get(10));

        // Internal map size is now 3, this should trigger history trimming.
        longVersionedValue.complete(2, TEST_VALUE);

        assertThrowsExactly(OutdatedTokenException.class, () -> longVersionedValue.get(0));
        assertDoesNotThrow(() -> longVersionedValue.get(10));
    }

    /**
     * Checks that future will be completed automatically when the related token becomes actual.
     */
    @Test
    public void testAutocompleteFuture() throws OutdatedTokenException {
        CompletableVersionedValue<Integer> longVersionedValue = new CompletableVersionedValue<>("test");

        longVersionedValue.complete(0, TEST_VALUE);

        CompletableFuture<Integer> fut = longVersionedValue.get(1);

        assertFalse(fut.isDone());

        longVersionedValue.complete(1);
        longVersionedValue.complete(2);

        assertTrue(fut.isDone());
        assertTrue(longVersionedValue.get(2).isDone());
    }

    /**
     * Test {@link CompletableVersionedValue#whenComplete}.
     */
    @Test
    public void testWhenComplete() {
        var vv = new CompletableVersionedValue<Integer>("test");

        CompletionListener<Integer> listener = mock(CompletionListener.class);

        vv.whenComplete(listener);

        // Test complete.
        long token = 0;

        vv.complete(token, TEST_VALUE);

        verify(listener).whenComplete(token, TEST_VALUE, null);

        // Test move revision.
        token = 1;

        vv.complete(token);

        verify(listener).whenComplete(token, TEST_VALUE, null);

        // Test complete exceptionally.
        token = 2;

        vv.completeExceptionally(token, TEST_EXCEPTION);

        verify(listener).whenComplete(token, null, TEST_EXCEPTION);

        // Test remove listener.
        token = 3;

        vv.removeWhenComplete(listener);

        clearInvocations(listener);

        vv.complete(token, TEST_VALUE);

        verify(listener, never()).whenComplete(anyLong(), any(), any());
    }

    /**
     * Checks a behavior when {@link CompletableVersionedValue} has not initialized yet, but someone already tries to get a value.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultValue() throws Exception {
        int defaultValue = 5;

        CompletableVersionedValue<Integer> longVersionedValue = new CompletableVersionedValue<>("test", () -> defaultValue);

        CompletableFuture<Integer> fut1 = longVersionedValue.get(1);
        CompletableFuture<Integer> fut2 = longVersionedValue.get(2);

        assertFalse(fut1.isDone());
        assertFalse(fut2.isDone());

        assertEquals(defaultValue, longVersionedValue.latest());

        longVersionedValue.complete(2, TEST_VALUE);

        assertTrue(fut1.isDone());
        assertTrue(fut2.isDone());

        assertEquals(5, fut1.get());
        assertEquals(TEST_VALUE, fut2.get());
    }

    @Test
    public void testLatest() {
        CompletableVersionedValue<Integer> vv = new CompletableVersionedValue<>("test");

        assertEquals(-1, vv.latestCausalityToken());

        vv.complete(1, 10);

        assertEquals(10, vv.latest());
        assertEquals(1, vv.latestCausalityToken());

        vv.complete(2);

        assertEquals(10, vv.latest());
        assertEquals(2, vv.latestCausalityToken());
    }

    @RepeatedTest(100)
    void testConcurrentGetAndComplete() throws Exception {
        var versionedValue = new CompletableVersionedValue<Integer>("test");

        // Set initial value.
        versionedValue.complete(1, 1);

        runRace(
                () -> versionedValue.complete(3, 3),
                () -> {
                    CompletableFuture<Integer> readerFuture = versionedValue.get(2);

                    assertThat(readerFuture, willBe(1));
                }
        );
    }

    @RepeatedTest(100)
    void testConcurrentGetAndCompleteWithHistoryTrimming() {
        var versionedValue = new CompletableVersionedValue<Integer>("test", 2);

        // Set initial value (history size 1).
        versionedValue.complete(2, 2);
        // Set history size to 2.
        versionedValue.complete(3, 3);

        runRace(
                // Trigger history trimming
                () -> versionedValue.complete(4, 4),
                () -> {
                    try {
                        CompletableFuture<Integer> readerFuture = versionedValue.get(2);

                        assertThat(readerFuture, willBe(2));
                    } catch (OutdatedTokenException ignored) {
                        // This is considered as a valid outcome.
                    }
                }
        );

        // Check that history has indeed been trimmed.
        assertThrows(OutdatedTokenException.class, () -> versionedValue.get(2));

        assertThat(versionedValue.get(4), willBe(4));
    }

    @Test
    void testCompleteMultipleFutures() {
        var versionedValue = new CompletableVersionedValue<Integer>("test");

        // Set initial value.
        versionedValue.complete(1, 1);

        CompletableFuture<Integer> future1 = versionedValue.get(2);
        CompletableFuture<Integer> future2 = versionedValue.get(3);
        CompletableFuture<Integer> future3 = versionedValue.get(4);

        versionedValue.complete(4, 2);

        assertThat(future1, willBe(1));
        assertThat(future2, willBe(1));
        assertThat(future3, willBe(2));
    }
}
