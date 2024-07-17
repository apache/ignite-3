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

/**
 * Lock mode.
 */
public enum LockMode {
    /** Not a lock. */
    NL,

    /** Intention shared. */
    IS,

    /** Intention exclusive. */
    IX,

    /** Shared. */
    S,

    /** Shared intention exclusive. */
    SIX,

    /** Exclusive. */
    X;

    /** Lock mode compatibility matrix. */
    private static final boolean[][] COMPAT_MATRIX = {
            {true, true, true, true, true, true},
            {true, true, true, true, true, false},
            {true, true, true, false, false, false},
            {true, true, false, true, false, false},
            {true, true, false, false, false, false},
            {true, false, false, false, false, false},
    };

    /** Lock mode reenter matrix. */
    private static final boolean[][] REENTER_MATRIX = {
            {true, false, false, false, false, false},
            {true, true, false, false, false, false},
            {true, true, true, false, false, false},
            {true, true, false, true, false, false},
            {true, true, true, true, true, false},
            {true, true, true, true, true, true},
    };

    /** Lock mode upgrade matrix. */
    private static final LockMode[][] UPGRADE_MATRIX = {
            {NL, IS, IX, S, SIX, X},
            {IS, IS, IX, S, SIX, X},
            {IX, IX, IX, SIX, SIX, X},
            {S, S, SIX, S, SIX, X},
            {SIX, SIX, SIX, SIX, SIX, X},
            {X, X, X, X, X, X},
    };

    /**
     * Is this lock mode compatible with the specified lock mode.
     *
     * @param lockMode Lock mode.
     * @return Is this lock mode compatible with the specified lock mode.
     */
    public boolean isCompatible(LockMode lockMode) {
        return COMPAT_MATRIX[ordinal()][lockMode.ordinal()];
    }

    /**
     * Is this lock mode can be reentered.
     *
     * @param lockMode Lock mode.
     * @return Is this lock mode can be reentered.
     */
    public boolean allowReenter(LockMode lockMode) {
        return REENTER_MATRIX[ordinal()][lockMode.ordinal()];
    }

    /**
     * Return the lock mode that is a supremum of the two given lock modes.
     *
     * @param lockMode1 Lock mode.
     * @param lockMode2 Lock mode.
     * @return The lock mode that is a supremum of the two given lock modes.
     */
    public static LockMode supremum(LockMode lockMode1, LockMode lockMode2) {
        return UPGRADE_MATRIX[lockMode1.ordinal()][lockMode2.ordinal()];
    }
}
