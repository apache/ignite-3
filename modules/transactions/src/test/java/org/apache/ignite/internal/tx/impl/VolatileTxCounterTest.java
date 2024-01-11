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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/** For {@link VolatileTxCounter} testing. */
public class VolatileTxCounterTest {
    private final VolatileTxCounter counter = new VolatileTxCounter();

    @Test
    void testIncrementTxCount() {
        counter.incrementTxCount(0);

        assertTrue(counter.isExistsTxBefore(1));
    }

    @Test
    void testDecrementTxCount() {
        counter.incrementTxCount(0);
        counter.decrementTxCount(0);

        assertFalse(counter.isExistsTxBefore(1));
    }

    @Test
    void testClear() {
        counter.incrementTxCount(0);

        counter.clear();

        assertFalse(counter.isExistsTxBefore(1));
    }

    @Test
    void testIsExistsTxBefore() {
        assertFalse(counter.isExistsTxBefore(0));
        assertFalse(counter.isExistsTxBefore(1));
        assertFalse(counter.isExistsTxBefore(2));

        counter.incrementTxCount(0);
        counter.incrementTxCount(1);
        counter.incrementTxCount(2);

        assertFalse(counter.isExistsTxBefore(0));
        assertTrue(counter.isExistsTxBefore(1));
        assertTrue(counter.isExistsTxBefore(2));

        counter.decrementTxCount(0);

        assertFalse(counter.isExistsTxBefore(0));
        assertFalse(counter.isExistsTxBefore(1));
        assertTrue(counter.isExistsTxBefore(2));

        counter.decrementTxCount(1);

        assertFalse(counter.isExistsTxBefore(0));
        assertFalse(counter.isExistsTxBefore(1));
        assertFalse(counter.isExistsTxBefore(2));
    }

    @Test
    void testSeveralIncrementsAndDecrementsOneCatalogVersion() {
        counter.incrementTxCount(0);
        counter.incrementTxCount(0);

        assertTrue(counter.isExistsTxBefore(1));

        counter.decrementTxCount(0);
        assertTrue(counter.isExistsTxBefore(1));

        counter.decrementTxCount(0);
        assertFalse(counter.isExistsTxBefore(1));

        counter.incrementTxCount(0);
        assertTrue(counter.isExistsTxBefore(1));

        counter.decrementTxCount(0);
        assertFalse(counter.isExistsTxBefore(1));
    }

    @Test
    void testConcurrentIncrementAndDecrementOneCatalogVersion() {
        CompletableFuture<?> future = runMultiThreadedAsync(
                () -> incrementAndDecrementTxCount(0, 100),
                3,
                "test"
        );

        assertThat(future, willCompleteSuccessfully());

        assertFalse(counter.isExistsTxBefore(1));
    }

    @Test
    void testConcurrentIncrementAndDecrementSeveralCatalogVersion() {
        CompletableFuture<?> future0 = runAsync(() -> incrementAndDecrementTxCount(0, 100));
        CompletableFuture<?> future1 = runAsync(() -> incrementAndDecrementTxCount(1, 100));
        CompletableFuture<?> future2 = runAsync(() -> incrementAndDecrementTxCount(2, 100));

        assertThat(CompletableFuture.allOf(future0, future1, future2), willCompleteSuccessfully());

        assertFalse(counter.isExistsTxBefore(1));
        assertFalse(counter.isExistsTxBefore(2));
        assertFalse(counter.isExistsTxBefore(3));
    }

    private void incrementAndDecrementTxCount(int catalogVersion, int count) {
        for (int i = 0; i < count; i++) {
            incrementAndDecrementTxCount(catalogVersion);
        }
    }

    private void incrementAndDecrementTxCount(int catalogVersion) {
        counter.incrementTxCount(catalogVersion);
        counter.decrementTxCount(catalogVersion);
    }
}
