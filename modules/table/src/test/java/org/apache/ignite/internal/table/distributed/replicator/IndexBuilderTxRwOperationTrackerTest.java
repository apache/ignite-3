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

package org.apache.ignite.internal.table.distributed.replicator;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** For {@link IndexBuilderTxRwOperationTracker} testing. */
public class IndexBuilderTxRwOperationTrackerTest {
    /** To be used in a loop. {@link RepeatedTest} has a smaller failure rate due to recreating the tracker every time. */
    private static final int REPEATS = 100;

    private final IndexBuilderTxRwOperationTracker tracker = new IndexBuilderTxRwOperationTracker();

    @AfterEach
    void tearDown() {
        tracker.close();
    }

    @Test
    void testAwaitCompleteTxRwOperationsNoOperations() {
        CompletableFuture<Void> awaitFuture0 = tracker.awaitCompleteTxRwOperations(0);
        CompletableFuture<Void> awaitFuture1 = tracker.awaitCompleteTxRwOperations(1);
        CompletableFuture<Void> awaitFuture2 = tracker.awaitCompleteTxRwOperations(1);

        assertFalse(awaitFuture0.isDone());
        assertFalse(awaitFuture1.isDone());
        assertFalse(awaitFuture2.isDone());

        tracker.updateMinAllowedCatalogVersionForStartOperation(0);

        assertTrue(awaitFuture0.isDone());
        assertFalse(awaitFuture1.isDone());
        assertFalse(awaitFuture2.isDone());

        tracker.updateMinAllowedCatalogVersionForStartOperation(1);

        assertTrue(awaitFuture1.isDone());
        assertTrue(awaitFuture2.isDone());
    }

    @Test
    void testAwaitCompleteTxRwOperationsWithOperations() {
        tracker.incrementOperationCount(0);
        tracker.incrementOperationCount(1);

        CompletableFuture<Void> awaitFuture0 = tracker.awaitCompleteTxRwOperations(0);
        CompletableFuture<Void> awaitFuture1 = tracker.awaitCompleteTxRwOperations(1);

        tracker.updateMinAllowedCatalogVersionForStartOperation(0);

        assertTrue(awaitFuture0.isDone());
        assertFalse(awaitFuture1.isDone());

        tracker.decrementOperationCount(1);
        assertFalse(awaitFuture1.isDone());

        tracker.updateMinAllowedCatalogVersionForStartOperation(1);
        assertFalse(awaitFuture1.isDone());

        tracker.decrementOperationCount(0);
        assertTrue(awaitFuture1.isDone());
    }

    @Test
    void testRejectTxRwOperation() {
        tracker.updateMinAllowedCatalogVersionForStartOperation(1);

        assertFalse(tracker.incrementOperationCount(0));

        assertTrue(tracker.incrementOperationCount(1));
        assertTrue(tracker.incrementOperationCount(2));
    }

    @Test
    void testConcurrentUpdateMinAllowedAndAwaitCompleteOperations0() {
        runRace(
                () -> {
                    for (int i = 0; i < REPEATS; i++) {
                        tracker.updateMinAllowedCatalogVersionForStartOperation(i);
                    }
                },
                () -> {
                    for (int i = 0; i < REPEATS; i++) {
                        assertThat(tracker.awaitCompleteTxRwOperations(i), willSucceedFast());
                    }
                },
                () -> {
                    for (int i = 0; i < REPEATS; i += 5) {
                        assertThat(tracker.awaitCompleteTxRwOperations(i), willSucceedFast());
                    }
                },
                () -> {
                    for (int i = 0; i < REPEATS; i += 10) {
                        assertThat(tracker.awaitCompleteTxRwOperations(i), willSucceedFast());
                    }
                }
        );
    }

    @Test
    void testConcurrentUpdateMinAllowedAndAwaitCompleteOperations1() {
        for (int i = 0; i < REPEATS; i++) {
            int catalogVersion = i;

            runRace(
                    () -> tracker.updateMinAllowedCatalogVersionForStartOperation(catalogVersion),
                    () -> assertThat(tracker.awaitCompleteTxRwOperations(catalogVersion), willSucceedFast())
            );
        }
    }

    @Test
    void testConcurrentIncrementAndDecrementOperationCount0() {
        runRace(
                () -> {
                    for (int i = 0; i < REPEATS; i++) {
                        tracker.incrementOperationCount(0);
                        tracker.decrementOperationCount(0);
                    }
                },
                () -> {
                    for (int i = 0; i < REPEATS; i++) {
                        tracker.incrementOperationCount(0);
                        tracker.decrementOperationCount(0);
                    }
                }
        );
    }

    @Test
    void testConcurrentIncrementAndDecrementOperationCount1() {
        for (int i = 0; i < REPEATS; i++) {
            runRace(
                    () -> {
                        tracker.incrementOperationCount(0);
                        tracker.decrementOperationCount(0);
                    },
                    () -> {
                        tracker.incrementOperationCount(0);
                        tracker.decrementOperationCount(0);
                    }
            );
        }
    }
}
