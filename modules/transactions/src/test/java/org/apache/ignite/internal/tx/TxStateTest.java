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

package org.apache.ignite.internal.tx;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Check {@link TxState} correctly validates transaction state changes.
 */
public class TxStateTest {

    @Test
    void testStates() {
        assertThat(TxState.values(),
                arrayContaining(TxState.PENDING, TxState.FINISHING, TxState.ABORTED, TxState.COMMITED, TxState.ABANDONED));
    }

    @Test
    void testFinalStates() {
        // Not final.
        assertFalse(TxState.isFinalState(TxState.PENDING));
        assertFalse(TxState.isFinalState(TxState.FINISHING));
        assertFalse(TxState.isFinalState(TxState.ABANDONED));

        // Final.
        assertTrue(TxState.isFinalState(TxState.ABORTED));
        assertTrue(TxState.isFinalState(TxState.COMMITED));
    }

    @Test
    void testTargetNullability() {
        assertThrows(NullPointerException.class, () -> TxState.checkTransitionCorrectness(TxState.PENDING, null));
    }

    @Test
    void testTransitionsFromNull() {
        // Not allowed.
        assertFalse(TxState.checkTransitionCorrectness(null, TxState.FINISHING));

        // Allowed.
        assertTrue(TxState.checkTransitionCorrectness(null, TxState.PENDING));
        assertTrue(TxState.checkTransitionCorrectness(null, TxState.ABORTED));
        assertTrue(TxState.checkTransitionCorrectness(null, TxState.COMMITED));
        assertTrue(TxState.checkTransitionCorrectness(null, TxState.ABANDONED));
    }

    @Test
    void testTransitionsFromPending() {
        // Allowed.
        assertTrue(TxState.checkTransitionCorrectness(TxState.PENDING, TxState.PENDING));
        assertTrue(TxState.checkTransitionCorrectness(TxState.PENDING, TxState.FINISHING));
        assertTrue(TxState.checkTransitionCorrectness(TxState.PENDING, TxState.ABORTED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.PENDING, TxState.COMMITED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.PENDING, TxState.ABANDONED));
    }

    @Test
    void testTransitionsFromFinishing() {
        // Not allowed.
        assertFalse(TxState.checkTransitionCorrectness(TxState.FINISHING, TxState.PENDING));
        assertFalse(TxState.checkTransitionCorrectness(TxState.FINISHING, TxState.FINISHING));

        // Allowed.
        assertTrue(TxState.checkTransitionCorrectness(TxState.FINISHING, TxState.ABORTED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.FINISHING, TxState.COMMITED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.FINISHING, TxState.ABANDONED));
    }

    @Test
    void testTransitionsFromAborted() {
        // Not allowed.
        assertFalse(TxState.checkTransitionCorrectness(TxState.ABORTED, TxState.PENDING));
        assertFalse(TxState.checkTransitionCorrectness(TxState.ABORTED, TxState.FINISHING));
        assertFalse(TxState.checkTransitionCorrectness(TxState.ABORTED, TxState.COMMITED));

        // Allowed.
        assertTrue(TxState.checkTransitionCorrectness(TxState.ABORTED, TxState.ABORTED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.ABORTED, TxState.ABANDONED));
    }

    @Test
    void testTransitionsFromCommitted() {
        // Not allowed.
        assertFalse(TxState.checkTransitionCorrectness(TxState.COMMITED, TxState.PENDING));
        assertFalse(TxState.checkTransitionCorrectness(TxState.COMMITED, TxState.FINISHING));
        assertFalse(TxState.checkTransitionCorrectness(TxState.COMMITED, TxState.ABORTED));

        // Allowed.
        assertTrue(TxState.checkTransitionCorrectness(TxState.COMMITED, TxState.COMMITED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.COMMITED, TxState.ABANDONED));
    }

    /**
     * Transition from ABANDONED to any state is allowed.
     */
    @Test
    void testTransitionsFromAbandoned() {
        // Allowed.
        assertTrue(TxState.checkTransitionCorrectness(TxState.ABANDONED, TxState.PENDING));
        assertTrue(TxState.checkTransitionCorrectness(TxState.ABANDONED, TxState.FINISHING));
        assertTrue(TxState.checkTransitionCorrectness(TxState.ABANDONED, TxState.ABORTED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.ABANDONED, TxState.COMMITED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.ABANDONED, TxState.ABANDONED));
    }
}
