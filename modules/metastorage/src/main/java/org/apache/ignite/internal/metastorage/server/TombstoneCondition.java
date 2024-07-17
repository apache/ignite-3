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

import org.apache.ignite.internal.metastorage.Entry;

/**
 * Condition tests an entry's value is tombstone in meta storage. Entry is tombstone if it is not empty and doesn't exists.
 */
public class TombstoneCondition extends AbstractSimpleCondition {
    /** Condition type. */
    private final TombstoneCondition.Type type;

    /**
     * Constructs a condition with the given entry key.
     *
     * @param type Condition type. Can't be {@code null}.
     * @param key Key identifies an entry which the condition will applied to.
     */
    public TombstoneCondition(TombstoneCondition.Type type, byte[] key) {
        super(key);

        this.type = type;
    }

    /** {@inheritDoc} */
    @Override
    public boolean test(Entry e) {
        return type.test(e.tombstone());
    }

    /** Defines tombstone condition types. */
    public enum Type {
        /** Tombstone condition type. */
        TOMBSTONE {
            @Override
            public boolean test(boolean res) {
                return res;
            }
        },

        /** Not tombstone condition type. */
        NOT_TOMBSTONE {
            @Override
            public boolean test(boolean res) {
                return !res;
            }
        };

        /**
         * Interprets comparison result.
         *
         * @param res The result of comparison.
         * @return The interpretation of the comparison result.
         */
        public abstract boolean test(boolean res);
    }
}
