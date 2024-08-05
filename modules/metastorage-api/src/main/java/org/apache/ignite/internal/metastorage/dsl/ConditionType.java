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

package org.apache.ignite.internal.metastorage.dsl;

/**
 * Defines possible condition types, which can be applied to a revision.
 */
public enum ConditionType {
    /** Equality condition type for a revision. */
    REV_EQUAL,

    /** Inequality condition type for a revision. */
    REV_NOT_EQUAL,

    /** Greater than condition type for a revision. */
    REV_GREATER,

    /** Less than condition type for a revision. */
    REV_LESS,

    /** Less than or equal to condition type for a revision. */
    REV_LESS_OR_EQUAL,

    /** Greater than or equal to condition type for a revision. */
    REV_GREATER_OR_EQUAL,

    /** Equality condition type for a value. */
    VAL_EQUAL,

    /** Inequality condition type for a value. */
    VAL_NOT_EQUAL,

    /** Greater than condition type for a value. */
    VAL_GREATER,

    /** Less than condition type for a value. */
    VAL_LESS,

    /** Less than or equal to condition type for a value. */
    VAL_LESS_OR_EQUAL,

    /** Greater than or equal to condition type for a value. */
    VAL_GREATER_OR_EQUAL,

    /** Existence condition type for a key. */
    KEY_EXISTS,

    /** Non-existence condition type for a key. */
    KEY_NOT_EXISTS,

    /** Tombstone condition type for a key. */
    TOMBSTONE,

    /** Not-tombstone condition type for a key. */
    NOT_TOMBSTONE;

    /** Cached array with all enum values. */
    private static final ConditionType[] VALUES = values();

    /**
     * Returns the enumerated value from its ordinal.
     *
     * @param ordinal Ordinal of enumeration constant.
     * @throws IllegalArgumentException If no enumeration constant by ordinal.
     */
    public static ConditionType fromOrdinal(int ordinal) throws IllegalArgumentException {
        if (ordinal < 0 || ordinal >= VALUES.length) {
            throw new IllegalArgumentException("No enum constant from ordinal: " + ordinal);
        }

        return VALUES[ordinal];
    }
}
