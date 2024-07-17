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

package org.apache.ignite.internal.metastorage.server;

import java.util.Arrays;
import org.apache.ignite.internal.metastorage.Entry;

/**
 * Defines condition on entry value. Values are compared in the lexicographical order.
 */
public class ValueCondition extends AbstractSimpleCondition {
    /** Condition type. */
    private final Type type;

    /** Value, which will be tested against an entry value. */
    private final byte[] val;

    /**
     * Constructs value condition with the given type, key and value.
     *
     * @param type Condition type. Can't be {@code null}.
     * @param key  Key identifies an entry, which condition will be applied to. Can't be {@code null}.
     * @param val  Value, which will be tested against an entry value. Can't be {@code null}.
     */
    public ValueCondition(Type type, byte[] key, byte[] val) {
        super(key);

        this.type = type;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override
    public boolean test(Entry e) {
        int res = Arrays.compare(e.value(), val);

        return type.test(res);
    }

    /**
     * Defines possible condition types, which can be applied to the value. Values are compared in the lexicographical order.
     */
    public enum Type {
        /** Equality condition type. */
        EQUAL {
            @Override
            public boolean test(long res) {
                return res == 0;
            }
        },

        /** Inequality condition type. */
        NOT_EQUAL {
            @Override
            public boolean test(long res) {
                return res != 0;
            }
        },

        /** Greater than condition type. */
        GREATER {
            @Override
            public boolean test(long res) {
                return res > 0;
            }
        },

        /** Less than condition type. */
        LESS {
            @Override
            public boolean test(long res) {
                return res < 0;
            }
        },

        /** Less than or equal to condition type. */
        LESS_OR_EQUAL {
            @Override
            public boolean test(long res) {
                return res <= 0;
            }
        },

        /** Greater than or equal to condition type. */
        GREATER_OR_EQUAL {
            @Override
            public boolean test(long res) {
                return res >= 0;
            }
        };

        /**
         * Interprets comparison result.
         *
         * @param res The result of comparison.
         * @return The interpretation of the comparison result.
         */
        public abstract boolean test(long res);
    }
}
