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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.booleanCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.completedOrFailedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;
import static org.apache.ignite.internal.util.CompletableFutures.emptyCollectionCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.emptyListCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.emptyMapCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.emptySetCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/** For {@link CompletableFutures} testing. */
public class CompletableFuturesTest {
    @Test
    void testNullCompletedFuture() {
        CompletableFuture<Integer> future = nullCompletedFuture();

        assertTrue(future.isDone());
        assertNull(future.join());
    }

    @Test
    void testTrueCompletedFuture() {
        CompletableFuture<Boolean> future = trueCompletedFuture();

        assertTrue(future.isDone());
        assertTrue(future.join());
    }

    @Test
    void testFalseCompletedFuture() {
        CompletableFuture<Boolean> future = falseCompletedFuture();

        assertTrue(future.isDone());
        assertFalse(future.join());
    }

    @Test
    void testBooleanCompletedFuture() {
        CompletableFuture<Boolean> future0 = booleanCompletedFuture(true);
        CompletableFuture<Boolean> future1 = booleanCompletedFuture(false);

        assertTrue(future0.isDone());
        assertTrue(future1.isDone());

        assertTrue(future0.join());
        assertFalse(future1.join());
    }

    @Test
    void testEmptyCollectionCompletedFuture() {
        CompletableFuture<Collection<String>> future = emptyCollectionCompletedFuture();

        assertTrue(future.isDone());
        assertTrue(future.join().isEmpty());

        assertThrows(UnsupportedOperationException.class, () -> future.join().add("1"));
    }

    @Test
    void testEmptyListCompletedFuture() {
        CompletableFuture<List<Integer>> future = emptyListCompletedFuture();

        assertTrue(future.isDone());
        assertTrue(future.join().isEmpty());

        assertThrows(UnsupportedOperationException.class, () -> future.join().add(1));
    }

    @Test
    void testEmptySetCompletedFuture() {
        CompletableFuture<Set<Long>> future = emptySetCompletedFuture();

        assertTrue(future.isDone());
        assertTrue(future.join().isEmpty());

        assertThrows(UnsupportedOperationException.class, () -> future.join().add(1L));
    }

    @Test
    void testEmptyMapCompletedFuture() {
        CompletableFuture<Map<Long, Integer>> future = emptyMapCompletedFuture();

        assertTrue(future.isDone());
        assertTrue(future.join().isEmpty());

        assertThrows(UnsupportedOperationException.class, () -> future.join().put(1L, 2));
    }

    @Test
    void testAllOfSuccessFuture() {
        CompletableFuture<List<Integer>> future = allOfToList(
                nullCompletedFuture(),
                completedFuture(1),
                completedFuture(42)
        );

        assertThat(future, will(contains(null, 1, 42)));
    }

    @Test
    void testAllFailedFuture() {
        CompletableFuture<List<Integer>> future = allOfToList(
                nullCompletedFuture(),
                failedFuture(new RuntimeException("test error")),
                completedFuture(42)
        );

        assertThat(future, willThrow(RuntimeException.class, "test error"));
    }

    @Test
    void completedOrFailedFutureCompletesSuccessfully() {
        assertThat(completedOrFailedFuture("ok", null), willBe("ok"));
    }

    @Test
    void completedOrFailedFutureCompletesWithNull() {
        assertThat(completedOrFailedFuture(null, null), willBe(nullValue()));
    }

    @Test
    void completedOrFailedFutureFails() {
        RuntimeException ex = new RuntimeException("Oops");
        RuntimeException caught = assertWillThrowFast(completedOrFailedFuture(null, ex), RuntimeException.class);

        assertThat(caught, is(ex));
    }

    @Test
    void testCopyStateToWithException() {
        var future0 = new CompletableFuture<String>();
        var future1 = new CompletableFuture<String>();

        var exception = new Exception("test");

        future0.whenComplete(copyStateTo(future1));
        future0.completeExceptionally(exception);

        assertThat(future1, willThrow(exception.getClass(), "test"));
    }

    @Test
    void testCopyStateTo() {
        var future0 = new CompletableFuture<String>();
        var future1 = new CompletableFuture<String>();

        future0.whenComplete(copyStateTo(future1));
        future0.complete("test");

        assertThat(future1, willBe("test"));
    }

    @Test
    void testAllOfEmpty() {
        CompletableFuture<Void> allOfFuture0 = CompletableFutures.allOf(List.of());
        CompletableFuture<Void> allOfFuture1 = CompletableFutures.allOf(Set.of());

        assertTrue(allOfFuture0.isDone());
        assertTrue(allOfFuture1.isDone());

        assertThat(allOfFuture0, willBe(nullValue()));
        assertThat(allOfFuture1, willBe(nullValue()));
    }

    @Test
    void testAllOfSuccessfully() {
        var future0 = new CompletableFuture<String>();
        var future1 = new CompletableFuture<Integer>();

        CompletableFuture<Void> allOfFuture = CompletableFutures.allOf(Set.of(future0, future1));
        assertFalse(allOfFuture.isDone());

        future1.complete(1);
        assertFalse(allOfFuture.isDone());

        future0.complete("test");
        assertTrue(allOfFuture.isDone());

        assertThat(allOfFuture, willBe(nullValue()));
    }

    @Test
    void testAllOfFailed() {
        var future0 = new CompletableFuture<String>();
        var future1 = new CompletableFuture<Integer>();

        CompletableFuture<Void> allOfFuture = CompletableFutures.allOf(List.of(future0, future1));
        assertFalse(allOfFuture.isDone());

        var exception0 = new Exception("from test 0");
        var exception1 = new Exception("from test 1");

        future0.completeExceptionally(exception0);
        assertFalse(allOfFuture.isDone());

        future1.completeExceptionally(exception1);
        assertTrue(allOfFuture.isDone());

        assertThat(allOfFuture, willThrow(Exception.class, "from test 0"));
    }
}
