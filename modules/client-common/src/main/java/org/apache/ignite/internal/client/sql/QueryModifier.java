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

package org.apache.ignite.internal.client.sql;

import java.util.EnumSet;
import java.util.Set;

/**
 * Classifier of SQL queries depending on the type of result returned.
 */
public enum QueryModifier {
    /** SELECT-like queries. */
    ALLOW_ROW_SET_RESULT(0),

    /** DML-like queries. */
    ALLOW_AFFECTED_ROWS_RESULT(1),

    /** DDL-like queries. */
    ALLOW_APPLIED_RESULT(2),

    /** Queries with transaction control statements. */
    ALLOW_TX_CONTROL(3),

    /** Queries with multiple statements. */
    ALLOW_MULTISTATEMENT(4);

    /** A set containing all modifiers. **/
    public static final Set<QueryModifier> ALL = EnumSet.allOf(QueryModifier.class);

    /** A set of modifiers that can apply to single statements. **/
    public static final Set<QueryModifier> SINGLE_STMT_MODIFIERS = EnumSet.complementOf(EnumSet.of(ALLOW_TX_CONTROL, ALLOW_MULTISTATEMENT));

    private static final QueryModifier[] VALS = new QueryModifier[values().length];

    static {
        for (QueryModifier type : values()) {
            assert VALS[type.id] == null : "Found duplicate id " + type.id;

            VALS[type.id] = type;
        }
    }

    private final int id;

    QueryModifier(int id) {
        this.id = id;
    }

    /** Packs a set of modifiers. */
    public static byte pack(Set<QueryModifier> modifiers) {
        assert VALS.length < 8 : "Packing more than 7 values is not supported";

        int result = 0;

        for (QueryModifier modifier : modifiers) {
            result = result | 1 << modifier.id;
        }

        return (byte) result;
    }

    /** Unpacks a set of modifiers. */
    public static Set<QueryModifier> unpack(byte data) {
        assert VALS.length < 8 : "Unpacking more than 7 values is not supported";

        Set<QueryModifier> modifiers = EnumSet.noneOf(QueryModifier.class);

        for (QueryModifier modifier : VALS) {
            int target = 1 << modifier.id;

            if ((target & data) == target) {
                modifiers.add(modifier);
            }
        }

        return modifiers;
    }
}
