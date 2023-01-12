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
 * Represents a condition for a meta storage conditional update.
 */
public class SimpleCondition implements Condition {
    /** Entry key. */
    private final byte[] key;

    /**
     * Condition type.
     */
    private final ConditionType type;

    private SimpleCondition(byte[] key, ConditionType type) {
        this.key = key;
        this.type = type;
    }

    /**
     * Returns the key, which identifies an entry, which condition will be applied to.
     *
     * @return Key, which identifies an entry, which condition will be applied to.
     */
    public byte[] key() {
        return key;
    }

    public ConditionType type() {
        return type;
    }

    public static RevisionConditionBuilder revision(byte[] key) {
        return new RevisionConditionBuilder(key);
    }

    public static ValueConditionBuilder value(byte[] key) {
        return new ValueConditionBuilder(key);
    }

    /**
     * Produces the condition of type {@link ConditionType#TOMBSTONE}. This condition tests that an entry's value, identified by the
     * given key, is tombstone.
     *
     * @return The condition of type {@link ConditionType#TOMBSTONE}.
     * @throws IllegalStateException In the case when the condition is already defined.
     */
    public static SimpleCondition tombstone(byte[] key) {
        return new SimpleCondition(key, ConditionType.TOMBSTONE);
    }

    /**
     * Produces the condition of type {@link ConditionType#KEY_EXISTS}. This condition tests the existence of an entry identified by the
     * given key.
     *
     * @return The condition of type {@link ConditionType#KEY_EXISTS}.
     * @throws IllegalStateException In the case when the condition is already defined.
     */
    public static SimpleCondition exists(byte[] key) {
        return new SimpleCondition(key, ConditionType.KEY_EXISTS);
    }

    /**
     * Produces the condition of type {@link ConditionType#KEY_NOT_EXISTS}. This condition tests the non-existence of an entry
     * identified by the given key.
     *
     * @return The condition of type {@link ConditionType#KEY_NOT_EXISTS}.
     * @throws IllegalStateException In the case when the condition is already defined.
     */
    public static SimpleCondition notExists(byte[] key) {
        return new SimpleCondition(key, ConditionType.KEY_NOT_EXISTS);
    }

    /**
     * Builder for {@link RevisionCondition}.
     */
    public static final class RevisionConditionBuilder {
        private final byte[] key;

        RevisionConditionBuilder(byte[] key) {
            this.key = key;
        }

        /**
         * Produces the condition of type {@link ConditionType#REV_EQUAL}. This condition tests the given revision on equality
         * with the target entry revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link ConditionType#REV_EQUAL}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition eq(long rev) {
            return new RevisionCondition(key, ConditionType.REV_EQUAL, rev);
        }

        /**
         * Produces the condition of type {@link ConditionType#REV_NOT_EQUAL}. This condition tests the given revision on inequality with
         * the target entry revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link ConditionType#REV_NOT_EQUAL}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition ne(long rev) {
            return new RevisionCondition(key, ConditionType.REV_NOT_EQUAL, rev);
        }

        /**
         * Produces the condition of type {@link ConditionType#REV_GREATER}. This condition tests that the target entry revision is greater
         * than the given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link ConditionType#REV_GREATER}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition gt(long rev) {
            return new RevisionCondition(key, ConditionType.REV_GREATER, rev);
        }

        /**
         * Produces the condition of type {@link ConditionType#REV_GREATER_OR_EQUAL}. This condition tests that the target entry revision is
         * greater than or equals to the given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link ConditionType#REV_GREATER_OR_EQUAL}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition ge(long rev) {
            return new RevisionCondition(key, ConditionType.REV_GREATER_OR_EQUAL, rev);
        }

        /**
         * Produces the condition of type {@link ConditionType#REV_LESS}. This condition tests that the target entry revision is less
         * than the given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link ConditionType#REV_LESS}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition lt(long rev) {
            return new RevisionCondition(key, ConditionType.REV_LESS, rev);
        }

        /**
         * Produces the condition of type {@link ConditionType#REV_LESS_OR_EQUAL}. This condition tests that the target entry revision
         * is less than or equals to the given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link ConditionType#REV_LESS_OR_EQUAL}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition le(long rev) {
            return new RevisionCondition(key, ConditionType.REV_LESS_OR_EQUAL, rev);
        }
    }

    /**
     * Represents a condition on an entry revision. Only one type of condition could be applied to the one instance of a condition.
     * Subsequent invocations of any method, which produces a condition will throw {@link IllegalStateException}.
     */
    public static final class RevisionCondition extends SimpleCondition {
        /** The revision as the condition argument. */
        private final long rev;

        /**
         * Constructs a condition by a revision for an entry identified by the given key.
         *
         * @param key Identifies an entry, which condition will be applied to.
         */
        RevisionCondition(byte[] key, ConditionType type, long rev) {
            super(key, type);

            assert rev > 0 : "Revision must be positive";

            this.rev = rev;
        }

        public long revision() {
            return rev;
        }
    }

    /**
     * Builder for {@link ValueCondition}.
     */
    public static final class ValueConditionBuilder {
        private final byte[] key;

        ValueConditionBuilder(byte[] key) {
            this.key = key;
        }

        /**
         * Produces the condition of type {@link ConditionType#VAL_EQUAL}. This condition tests the given value on equality with the target
         * entry value.
         *
         * @param val The value.
         * @return The condition of type {@link ConditionType#VAL_EQUAL}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition eq(byte[] val) {
            return new ValueCondition(key, ConditionType.VAL_EQUAL, val);
        }

        /**
         * Produces the condition of type {@link ConditionType#VAL_NOT_EQUAL}. This condition tests the given value on inequality with
         * the target entry value.
         *
         * @param val The value.
         * @return The condition of type {@link ConditionType#VAL_NOT_EQUAL}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition ne(byte[] val) {
            return new ValueCondition(key, ConditionType.VAL_NOT_EQUAL, val);
        }


        /**
         * Produces the condition of type {@link ConditionType#VAL_GREATER}. This condition tests that the target entry value is greater
         * than the given value in the lexicographical order.
         *
         * @param val The value.
         * @return The condition of type {@link ConditionType#VAL_GREATER}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition gt(byte[] val) {
            return new ValueCondition(key, ConditionType.VAL_GREATER, val);
        }

        /**
         * Produces the condition of type {@link ConditionType#VAL_GREATER_OR_EQUAL}. This condition tests that the target entry value is
         * greater than or equals to the given value in the lexicographical order.
         *
         * @param val The value.
         * @return The condition of type {@link ConditionType#VAL_GREATER_OR_EQUAL}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition ge(byte[] val) {
            return new ValueCondition(key, ConditionType.VAL_GREATER_OR_EQUAL, val);
        }

        /**
         * Produces the condition of type {@link ConditionType#VAL_LESS}. This condition tests that the target entry value is less than the
         * given value in the lexicographical order.
         *
         * @param val The value.
         * @return The condition of type {@link ConditionType#VAL_LESS}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition lt(byte[] val) {
            return new ValueCondition(key, ConditionType.VAL_LESS, val);
        }

        /**
         * Produces the condition of type {@link ConditionType#VAL_LESS_OR_EQUAL}. This condition tests that the target entry value is less
         * than or equals to the given value in the lexicographical order.
         *
         * @param val The value.
         * @return The condition of type {@link ConditionType#VAL_LESS_OR_EQUAL}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition le(byte[] val) {
            return new ValueCondition(key, ConditionType.VAL_LESS_OR_EQUAL, val);
        }
    }

    /**
     * Represents a condition on an entry value. Only the one type of condition could be applied to the one instance of a condition.
     * Subsequent invocations of any method, which produces a condition will throw {@link IllegalStateException}.
     */
    public static final class ValueCondition extends SimpleCondition {
        /** The value as the condition argument. */
        private final byte[] val;

        /**
         * Constructs a condition by a value for an entry identified by the given key.
         *
         * @param key Identifies an entry, which condition will be applied to.
         */
        ValueCondition(byte[] key, ConditionType type, byte[] val) {
            super(key, type);

            this.val = val;
        }

        public byte[] value() {
            return val;
        }
    }
}
