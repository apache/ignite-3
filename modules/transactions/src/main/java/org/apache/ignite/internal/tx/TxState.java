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

import static java.util.Objects.requireNonNull;

import org.jetbrains.annotations.Nullable;

/**
 * Transaction state.
 */
public enum TxState {
    /**
     * Active transaction that is in progress.
     */
    PENDING,

    /**
     * Transaction can be put in this state on a transaction coordinator or a commit partition on a start of finalization process
     * (for commit partition this is true only in case of recovery, when the commit partition initiates this finalization process)
     * and the transaction ends up with some final state ({@link #COMMITTED} or {@link #ABORTED}) when receives a tx finish response from
     * commit partition on a coordinator, or finishes the transaction recovery on a commit partition. This state can be also seen locally
     * on data nodes if they are colocated with the coordinator or the commit partition.
     */
    FINISHING,

    /**
     * Aborted (rolled back) transaction.
     */
    ABORTED,

    /**
     * Committed transaction.
     */
    COMMITTED,

    /**
     * State that is assigned to a transaction due to absence of coordinator. It is temporary and can be changed to
     * {@link TxState#COMMITTED} or {@link TxState#ABORTED} after recovery or successful write intent resolution.
     */
    ABANDONED;

    private static final boolean[][] TRANSITION_MATRIX = {
            { false, true,  true, true,  true,  true },
            { false, true,  true,  true,  true,  true },
            { false, false, false, true,  true,  true },
            { false, false, false, true,  false, false },
            { false, false, false, false, true,  false },
            { false,  false,  true,  true,  true,  true }
    };

    /** Cached array with all enum values. */
    private static final TxState[] VALUES = values();

    /**
     * Checks whether the state is final, i.e. no transition from this state is allowed.
     *
     * @param state Transaction state.
     * @return {@code true} if the state is either {@link #COMMITTED} or {@link #ABORTED}
     */
    public static boolean isFinalState(TxState state) {
        return state == COMMITTED || state == ABORTED;
    }

    /**
     * Checks the correctness of the transition between transaction states.
     *
     * @param before State before.
     * @param after State after.
     * @return Whether the transition is correct.
     */
    public static boolean checkTransitionCorrectness(@Nullable TxState before, TxState after) {
        requireNonNull(after);

        int beforeOrd = before == null ? 0 : before.ordinal() + 1;
        int afterOrd = after.ordinal() + 1;

        return TRANSITION_MATRIX[beforeOrd][afterOrd];
    }

    /**
     * Returns the enumerated value from its ordinal.
     *
     * @param ordinal Ordinal of enumeration constant.
     * @throws IllegalArgumentException If no enumeration constant by ordinal.
     */
    public static TxState fromOrdinal(int ordinal) throws IllegalArgumentException {
        if (ordinal < 0 || ordinal >= VALUES.length) {
            throw new IllegalArgumentException("No enum constant from ordinal: " + ordinal);
        }

        return VALUES[ordinal];
    }
}
