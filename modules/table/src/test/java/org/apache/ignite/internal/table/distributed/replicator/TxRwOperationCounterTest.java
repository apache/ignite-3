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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** For {@link TxRwOperationCounter} testing. */
public class TxRwOperationCounterTest {
    @Test
    void testWithCountOne() {
        var counter = TxRwOperationCounter.withCountOne();

        assertOperationsIsNotComplete(counter);
    }

    @Test
    void testDecrement() {
        var counter0 = TxRwOperationCounter.withCountOne();

        TxRwOperationCounter counter1 = counter0.decrementOperationCount();

        assertNotSame(counter0, counter1);

        assertSame(counter0.operationsFuture(), counter1.operationsFuture());

        assertOperationsIsNotComplete(counter0);

        assertTrue(counter1.isOperationsOver());
        assertFalse(counter1.operationsFuture().isDone());
    }

    @Test
    void testIncrement() {
        var counter0 = TxRwOperationCounter.withCountOne();

        TxRwOperationCounter counter1 = counter0.incrementOperationCount();

        assertNotSame(counter0, counter1);

        assertSame(counter0.operationsFuture(), counter1.operationsFuture());

        assertOperationsIsNotComplete(counter0);
        assertOperationsIsNotComplete(counter1);
    }

    @Test
    void testSeveralOperation() {
        TxRwOperationCounter counter = TxRwOperationCounter.withCountOne().incrementOperationCount();
        assertOperationsIsNotComplete(counter);

        counter = counter.incrementOperationCount();
        assertOperationsIsNotComplete(counter);

        counter = counter.decrementOperationCount();
        assertOperationsIsNotComplete(counter);

        counter = counter.decrementOperationCount();
        assertOperationsIsNotComplete(counter);

        counter = counter.decrementOperationCount();
        assertTrue(counter.isOperationsOver());
        assertFalse(counter.operationsFuture().isDone());
    }

    private static void assertOperationsIsNotComplete(TxRwOperationCounter counter) {
        assertFalse(counter.isOperationsOver());
        assertFalse(counter.operationsFuture().isDone());
    }
}
