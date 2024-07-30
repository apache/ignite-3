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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.network.NetworkMessage;
import org.junit.jupiter.api.Test;

/** For {@link TxState} testing. */
public class TxStateTest {
    @Test
    void testStates() {
        assertThat(TxState.values(),
                arrayContaining(TxState.PENDING, TxState.FINISHING, TxState.ABORTED, TxState.COMMITTED, TxState.ABANDONED));
    }

    @Test
    void testFinalStates() {
        // Not final.
        assertFalse(TxState.isFinalState(TxState.PENDING));
        assertFalse(TxState.isFinalState(TxState.FINISHING));
        assertFalse(TxState.isFinalState(TxState.ABANDONED));

        // Final.
        assertTrue(TxState.isFinalState(TxState.ABORTED));
        assertTrue(TxState.isFinalState(TxState.COMMITTED));
    }

    @Test
    void testTargetNullability() {
        assertThrows(NullPointerException.class, () -> TxState.checkTransitionCorrectness(TxState.PENDING, null));
    }

    @Test
    void testTransitionsFromNull() {
        // Allowed.
        assertTrue(TxState.checkTransitionCorrectness(null, TxState.PENDING));
        assertTrue(TxState.checkTransitionCorrectness(null, TxState.ABORTED));
        assertTrue(TxState.checkTransitionCorrectness(null, TxState.COMMITTED));
        assertTrue(TxState.checkTransitionCorrectness(null, TxState.ABANDONED));
        assertTrue(TxState.checkTransitionCorrectness(null, TxState.FINISHING));
    }

    @Test
    void testTransitionsFromPending() {
        // Allowed.
        assertTrue(TxState.checkTransitionCorrectness(TxState.PENDING, TxState.PENDING));
        assertTrue(TxState.checkTransitionCorrectness(TxState.PENDING, TxState.FINISHING));
        assertTrue(TxState.checkTransitionCorrectness(TxState.PENDING, TxState.ABORTED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.PENDING, TxState.COMMITTED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.PENDING, TxState.ABANDONED));
    }

    @Test
    void testTransitionsFromFinishing() {
        // Not allowed.
        assertFalse(TxState.checkTransitionCorrectness(TxState.FINISHING, TxState.PENDING));
        assertFalse(TxState.checkTransitionCorrectness(TxState.FINISHING, TxState.FINISHING));

        // Allowed.
        assertTrue(TxState.checkTransitionCorrectness(TxState.FINISHING, TxState.ABORTED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.FINISHING, TxState.COMMITTED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.FINISHING, TxState.ABANDONED));
    }

    @Test
    void testTransitionsFromAborted() {
        // Not allowed.
        assertFalse(TxState.checkTransitionCorrectness(TxState.ABORTED, TxState.PENDING));
        assertFalse(TxState.checkTransitionCorrectness(TxState.ABORTED, TxState.FINISHING));
        assertFalse(TxState.checkTransitionCorrectness(TxState.ABORTED, TxState.COMMITTED));
        assertFalse(TxState.checkTransitionCorrectness(TxState.ABORTED, TxState.ABANDONED));

        // Allowed.
        assertTrue(TxState.checkTransitionCorrectness(TxState.ABORTED, TxState.ABORTED));
    }

    @Test
    void testTransitionsFromCommitted() {
        // Not allowed.
        assertFalse(TxState.checkTransitionCorrectness(TxState.COMMITTED, TxState.PENDING));
        assertFalse(TxState.checkTransitionCorrectness(TxState.COMMITTED, TxState.FINISHING));
        assertFalse(TxState.checkTransitionCorrectness(TxState.COMMITTED, TxState.ABORTED));
        assertFalse(TxState.checkTransitionCorrectness(TxState.COMMITTED, TxState.ABANDONED));

        // Allowed.
        assertTrue(TxState.checkTransitionCorrectness(TxState.COMMITTED, TxState.COMMITTED));
    }

    /**
     * Transition from ABANDONED to any state is allowed.
     */
    @Test
    void testTransitionsFromAbandoned() {
        // Not allowed.
        assertFalse(TxState.checkTransitionCorrectness(TxState.ABANDONED, TxState.PENDING));

        // Allowed.
        assertTrue(TxState.checkTransitionCorrectness(TxState.ABANDONED, TxState.FINISHING));
        assertTrue(TxState.checkTransitionCorrectness(TxState.ABANDONED, TxState.ABORTED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.ABANDONED, TxState.COMMITTED));
        assertTrue(TxState.checkTransitionCorrectness(TxState.ABANDONED, TxState.ABANDONED));
    }

    /** Checks that the ordinal does not change, since the enum will be transfer in the {@link NetworkMessage}. */
    @Test
    void testFromOrdinal() {
        assertEquals(TxState.PENDING, TxState.fromOrdinal(0));

        assertEquals(TxState.FINISHING, TxState.fromOrdinal(1));

        assertEquals(TxState.ABORTED, TxState.fromOrdinal(2));

        assertEquals(TxState.COMMITTED, TxState.fromOrdinal(3));

        assertEquals(TxState.ABANDONED, TxState.fromOrdinal(4));

        assertThrows(IllegalArgumentException.class, () -> TxState.fromOrdinal(-1));
        assertThrows(IllegalArgumentException.class, () -> TxState.fromOrdinal(5));
    }
}
