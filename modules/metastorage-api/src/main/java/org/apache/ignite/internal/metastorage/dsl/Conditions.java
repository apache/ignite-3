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

import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition.RevisionCondition;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition.ValueCondition;

/**
 * This class contains fabric methods which produce conditions needed for a conditional multi update functionality provided by the meta
 * storage service.
 *
 * @see SimpleCondition
 */
public final class Conditions {
    private static final MetaStorageMessagesFactory MSG_FACTORY = new MetaStorageMessagesFactory();

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
         */
        public SimpleCondition eq(long rev) {
            return MSG_FACTORY.revisionCondition()
                    .key(key)
                    .conditionType(ConditionType.REV_EQUAL.ordinal())
                    .revision(rev)
                    .build();
        }

        /**
         * Produces the condition of type {@link ConditionType#REV_NOT_EQUAL}. This condition tests the given revision on inequality with
         * the target entry revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link ConditionType#REV_NOT_EQUAL}.
         */
        public SimpleCondition ne(long rev) {
            return MSG_FACTORY.revisionCondition()
                    .key(key)
                    .conditionType(ConditionType.REV_NOT_EQUAL.ordinal())
                    .revision(rev)
                    .build();
        }

        /**
         * Produces the condition of type {@link ConditionType#REV_GREATER}. This condition tests that the target entry revision is greater
         * than the given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link ConditionType#REV_GREATER}.
         */
        public SimpleCondition gt(long rev) {
            return MSG_FACTORY.revisionCondition()
                    .key(key)
                    .conditionType(ConditionType.REV_GREATER.ordinal())
                    .revision(rev)
                    .build();
        }

        /**
         * Produces the condition of type {@link ConditionType#REV_GREATER_OR_EQUAL}. This condition tests that the target entry revision is
         * greater than or equals to the given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link ConditionType#REV_GREATER_OR_EQUAL}.
         */
        public SimpleCondition ge(long rev) {
            return MSG_FACTORY.revisionCondition()
                    .key(key)
                    .conditionType(ConditionType.REV_GREATER_OR_EQUAL.ordinal())
                    .revision(rev)
                    .build();
        }

        /**
         * Produces the condition of type {@link ConditionType#REV_LESS}. This condition tests that the target entry revision is less
         * than the given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link ConditionType#REV_LESS}.
         */
        public SimpleCondition lt(long rev) {
            return MSG_FACTORY.revisionCondition()
                    .key(key)
                    .conditionType(ConditionType.REV_LESS.ordinal())
                    .revision(rev)
                    .build();
        }

        /**
         * Produces the condition of type {@link ConditionType#REV_LESS_OR_EQUAL}. This condition tests that the target entry revision
         * is less than or equals to the given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link ConditionType#REV_LESS_OR_EQUAL}.
         */
        public SimpleCondition le(long rev) {
            return MSG_FACTORY.revisionCondition()
                    .key(key)
                    .conditionType(ConditionType.REV_LESS_OR_EQUAL.ordinal())
                    .revision(rev)
                    .build();
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
         */
        public SimpleCondition eq(byte[] val) {
            return MSG_FACTORY.valueCondition()
                    .key(key)
                    .conditionType(ConditionType.VAL_EQUAL.ordinal())
                    .value(val)
                    .build();
        }

        /**
         * Produces the condition of type {@link ConditionType#VAL_NOT_EQUAL}. This condition tests the given value on inequality with
         * the target entry value.
         *
         * @param val The value.
         * @return The condition of type {@link ConditionType#VAL_NOT_EQUAL}.
         */
        public SimpleCondition ne(byte[] val) {
            return MSG_FACTORY.valueCondition()
                    .key(key)
                    .conditionType(ConditionType.VAL_NOT_EQUAL.ordinal())
                    .value(val)
                    .build();
        }

        /**
         * Produces the condition of type {@link ConditionType#VAL_GREATER}. This condition tests that the target entry value is greater
         * than the given value in the lexicographical order.
         *
         * @param val The value.
         * @return The condition of type {@link ConditionType#VAL_GREATER}.
         */
        public SimpleCondition gt(byte[] val) {
            return MSG_FACTORY.valueCondition()
                    .key(key)
                    .conditionType(ConditionType.VAL_GREATER.ordinal())
                    .value(val)
                    .build();
        }

        /**
         * Produces the condition of type {@link ConditionType#VAL_GREATER_OR_EQUAL}. This condition tests that the target entry value is
         * greater than or equals to the given value in the lexicographical order.
         *
         * @param val The value.
         * @return The condition of type {@link ConditionType#VAL_GREATER_OR_EQUAL}.
         */
        public SimpleCondition ge(byte[] val) {
            return MSG_FACTORY.valueCondition()
                    .key(key)
                    .conditionType(ConditionType.VAL_GREATER_OR_EQUAL.ordinal())
                    .value(val)
                    .build();
        }

        /**
         * Produces the condition of type {@link ConditionType#VAL_LESS}. This condition tests that the target entry value is less than the
         * given value in the lexicographical order.
         *
         * @param val The value.
         * @return The condition of type {@link ConditionType#VAL_LESS}.
         */
        public SimpleCondition lt(byte[] val) {
            return MSG_FACTORY.valueCondition()
                    .key(key)
                    .conditionType(ConditionType.VAL_LESS.ordinal())
                    .value(val)
                    .build();
        }

        /**
         * Produces the condition of type {@link ConditionType#VAL_LESS_OR_EQUAL}. This condition tests that the target entry value is less
         * than or equals to the given value in the lexicographical order.
         *
         * @param val The value.
         * @return The condition of type {@link ConditionType#VAL_LESS_OR_EQUAL}.
         */
        public SimpleCondition le(byte[] val) {
            return MSG_FACTORY.valueCondition()
                    .key(key)
                    .conditionType(ConditionType.VAL_LESS_OR_EQUAL.ordinal())
                    .value(val)
                    .build();
        }
    }

    /**
     * Creates a builder for a value condition.
     *
     * @param key Key, which identifies an entry, which condition will be applied to.
     * @return Builder for a value condition.
     * @see ValueCondition
     */
    public static ValueConditionBuilder value(ByteArray key) {
        return new ValueConditionBuilder(key.bytes());
    }

    /**
     * Produces the condition of type {@link ConditionType#TOMBSTONE}. This condition tests that an entry's value, identified by the
     * given key, is tombstone.
     *
     * @return The condition of type {@link ConditionType#TOMBSTONE}.
     */
    public static SimpleCondition tombstone(ByteArray key) {
        return MSG_FACTORY.simpleCondition()
                .key(key.bytes())
                .conditionType(ConditionType.TOMBSTONE.ordinal())
                .build();
    }

    /**
     * Produces the condition of type {@link ConditionType#NOT_TOMBSTONE}. This condition tests that an entry's value, identified by the
     * given key, is not a tombstone.
     *
     * @return The condition of type {@link ConditionType#NOT_TOMBSTONE}.
     */
    public static SimpleCondition notTombstone(ByteArray key) {
        return MSG_FACTORY.simpleCondition()
                .key(key.bytes())
                .conditionType(ConditionType.NOT_TOMBSTONE.ordinal())
                .build();
    }

    /**
     * Produces the condition of type {@link ConditionType#KEY_EXISTS}. This condition tests the existence of an entry identified by the
     * given key.
     *
     * @return The condition of type {@link ConditionType#KEY_EXISTS}.
     */
    public static SimpleCondition exists(ByteArray key) {
        return MSG_FACTORY.simpleCondition()
                .key(key.bytes())
                .conditionType(ConditionType.KEY_EXISTS.ordinal())
                .build();
    }

    /**
     * Produces the condition of type {@link ConditionType#KEY_NOT_EXISTS}. This condition tests the non-existence of an entry
     * identified by the given key.
     *
     * @return The condition of type {@link ConditionType#KEY_NOT_EXISTS}.
     */
    public static SimpleCondition notExists(ByteArray key) {
        return MSG_FACTORY.simpleCondition()
                .key(key.bytes())
                .conditionType(ConditionType.KEY_NOT_EXISTS.ordinal())
                .build();
    }

    /**
     * Creates condition on entry revision.
     *
     * @param key Identifies an entry which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry revision.
     * @see SimpleCondition.RevisionCondition
     */
    public static RevisionConditionBuilder revision(ByteArray key) {
        return new RevisionConditionBuilder(key.bytes());
    }

    /**
     * Shortcut for create logical AND condition,
     * which consists two another simple/compound conditions as left and right operands.
     *
     * @param leftCond leftCond condition.
     * @param rightCond right condition.
     * @return new AND compound condition.
     */
    public static CompoundCondition and(Condition leftCond, Condition rightCond) {
        return MSG_FACTORY.compoundCondition()
                .leftCondition(leftCond)
                .rightCondition(rightCond)
                .compoundConditionType(CompoundConditionType.AND.ordinal())
                .build();
    }

    /**
     * Shortcut for create logical OR condition,
     * which consists two another simple/compound conditions as left and right operands.
     *
     * @param leftCond leftCond condition.
     * @param rightCond right condition.
     * @return new OR compound condition.
     */
    public static CompoundCondition or(Condition leftCond, Condition rightCond) {
        return MSG_FACTORY.compoundCondition()
                .leftCondition(leftCond)
                .rightCondition(rightCond)
                .compoundConditionType(CompoundConditionType.OR.ordinal())
                .build();
    }

    /**
     * Default no-op constructor.
     */
    private Conditions() {
        // No-op.
    }
}
