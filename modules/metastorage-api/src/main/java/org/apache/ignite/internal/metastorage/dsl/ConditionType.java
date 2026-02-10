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
    REV_EQUAL(0),

    /** Inequality condition type for a revision. */
    REV_NOT_EQUAL(1),

    /** Greater than condition type for a revision. */
    REV_GREATER(2),

    /** Less than condition type for a revision. */
    REV_LESS(3),

    /** Less than or equal to condition type for a revision. */
    REV_LESS_OR_EQUAL(4),

    /** Greater than or equal to condition type for a revision. */
    REV_GREATER_OR_EQUAL(5),

    /** Equality condition type for a value. */
    VAL_EQUAL(6),

    /** Inequality condition type for a value. */
    VAL_NOT_EQUAL(7),

    /** Greater than condition type for a value. */
    VAL_GREATER(8),

    /** Less than condition type for a value. */
    VAL_LESS(9),

    /** Less than or equal to condition type for a value. */
    VAL_LESS_OR_EQUAL(10),

    /** Greater than or equal to condition type for a value. */
    VAL_GREATER_OR_EQUAL(11),

    /** Existence condition type for a key. */
    KEY_EXISTS(12),

    /** Non-existence condition type for a key. */
    KEY_NOT_EXISTS(13),

    /** Tombstone condition type for a key. */
    TOMBSTONE(14),

    /** Not-tombstone condition type for a key. */
    NOT_TOMBSTONE(15);

    private final int id;

    ConditionType(int id) {
        this.id = id;
    }

    /**
     * Returns the enumerated value from its id.
     *
     * @param id Id of enumeration constant.
     * @throws IllegalArgumentException If no enumeration constant by id.
     */
    public static ConditionType fromId(int id) throws IllegalArgumentException {
        switch (id) {
            case 0: return REV_EQUAL;
            case 1: return REV_NOT_EQUAL;
            case 2: return REV_GREATER;
            case 3: return REV_LESS;
            case 4: return REV_LESS_OR_EQUAL;
            case 5: return REV_GREATER_OR_EQUAL;
            case 6: return VAL_EQUAL;
            case 7: return VAL_NOT_EQUAL;
            case 8: return VAL_GREATER;
            case 9: return VAL_LESS;
            case 10: return VAL_LESS_OR_EQUAL;
            case 11: return VAL_GREATER_OR_EQUAL;
            case 12: return KEY_EXISTS;
            case 13: return KEY_NOT_EXISTS;
            case 14: return TOMBSTONE;
            case 15: return NOT_TOMBSTONE;
            default:
                throw new IllegalArgumentException("No enum constant from id: " + id);
        }
    }

    public int id() {
        return id;
    }
}
