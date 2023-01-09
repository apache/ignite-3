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
public final class SimpleCondition implements Condition {
    /** Actual condition implementation. */
    private final InnerCondition cond;

    /**
     * Constructs a condition, which wraps the actual condition implementation.
     *
     * @param cond The actual condition implementation.
     */
    SimpleCondition(InnerCondition cond) {
        this.cond = cond;
    }

    public InnerCondition inner() {
        return cond;
    }

    public ConditionType type() {
        return cond.type();
    }

    /**
     * Represents a condition on an entry revision. Only one type of condition could be applied to the one instance of a condition.
     * Subsequent invocations of any method, which produces a condition will throw {@link IllegalStateException}.
     */
    public static final class RevisionCondition extends AbstractCondition {
        /** The revision as the condition argument. */
        private long rev;

        /**
         * Constructs a condition by a revision for an entry identified by the given key.
         *
         * @param key Identifies an entry, which condition will be applied to.
         */
        RevisionCondition(byte[] key) {
            super(key);
        }

        public long revision() {
            return rev;
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
            assert rev > 0 : "Revision must be positive.";

            validate(type());

            type(ConditionType.REV_EQUAL);

            this.rev = rev;

            return new SimpleCondition(this);
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
            assert rev > 0 : "Revision must be positive.";

            validate(type());

            type(ConditionType.REV_NOT_EQUAL);

            this.rev = rev;

            return new SimpleCondition(this);
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
            assert rev > 0 : "Revision must be positive.";

            validate(type());

            type(ConditionType.REV_GREATER);

            this.rev = rev;

            return new SimpleCondition(this);
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
            assert rev > 0 : "Revision must be positive.";

            validate(type());

            type(ConditionType.REV_GREATER_OR_EQUAL);

            this.rev = rev;

            return new SimpleCondition(this);
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
            assert rev > 0 : "Revision must be positive.";

            validate(type());

            type(ConditionType.REV_LESS);

            this.rev = rev;

            return new SimpleCondition(this);
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
            assert rev > 0 : "Revision must be positive.";

            validate(type());

            type(ConditionType.REV_LESS_OR_EQUAL);

            this.rev = rev;

            return new SimpleCondition(this);
        }
    }

    /**
     * Represents a condition on an entry value. Only the one type of condition could be applied to the one instance of a condition.
     * Subsequent invocations of any method, which produces a condition will throw {@link IllegalStateException}.
     */
    public static final class ValueCondition extends AbstractCondition {
        /** The value as the condition argument. */
        private byte[] val;

        /**
         * Constructs a condition by a value for an entry identified by the given key.
         *
         * @param key Identifies an entry, which condition will be applied to.
         */
        ValueCondition(byte[] key) {
            super(key);
        }

        public byte[] value() {
            return val;
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
            validate(type());

            type(ConditionType.VAL_EQUAL);

            this.val = val;

            return new SimpleCondition(this);
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
            validate(type());

            type(ConditionType.VAL_NOT_EQUAL);

            this.val = val;

            return new SimpleCondition(this);
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
            validate(type());

            type(ConditionType.VAL_GREATER);

            this.val = val;

            return new SimpleCondition(this);
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
            validate(type());

            type(ConditionType.VAL_GREATER_OR_EQUAL);

            this.val = val;

            return new SimpleCondition(this);
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
            validate(type());

            type(ConditionType.VAL_LESS);

            this.val = val;

            return new SimpleCondition(this);
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
            validate(type());

            type(ConditionType.VAL_LESS_OR_EQUAL);

            this.val = val;

            return new SimpleCondition(this);
        }
    }

    /**
     * Represents a condition on an entry existence. Only the one type of a condition could be applied to the one instance of a condition.
     * Subsequent invocations of any method, which produces a condition will throw {@link IllegalStateException}.
     */
    public static final class ExistenceCondition extends AbstractCondition {
        /**
         * Constructs a condition on existence an entry identified by the given key.
         *
         * @param key Identifies an entry, which condition will be applied to.
         */
        ExistenceCondition(byte[] key) {
            super(key);
        }

        /**
         * Produces the condition of type {@link ConditionType#KEY_EXISTS}. This condition tests the existence of an entry identified by the
         * given key.
         *
         * @return The condition of type {@link ConditionType#KEY_EXISTS}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition exists() {
            validate(type());

            type(ConditionType.KEY_EXISTS);

            return new SimpleCondition(this);
        }

        /**
         * Produces the condition of type {@link ConditionType#KEY_NOT_EXISTS}. This condition tests the non-existence of an entry
         * identified by the given key.
         *
         * @return The condition of type {@link ConditionType#KEY_NOT_EXISTS}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition notExists() {
            validate(type());

            type(ConditionType.KEY_NOT_EXISTS);

            return new SimpleCondition(this);
        }
    }

    /**
     * Represents a condition on an entry's value, which checks whether a value is tombstone or not. Only the one type of condition
     * could be applied to the one instance of a condition. Subsequent invocations of any method which produces a condition
     * will throw {@link IllegalStateException}.
     */
    public static final class TombstoneCondition extends AbstractCondition {
        /**
         * Constructs a condition on an entry, identified by the given key, is tombstone.
         *
         * @param key Identifies an entry, which condition will be applied to.
         */
        TombstoneCondition(byte[] key) {
            super(key);
        }

        /**
         * Produces the condition of type {@link ConditionType#TOMBSTONE}. This condition tests that an entry's value, identified by the
         * given key, is tombstone.
         *
         * @return The condition of type {@link ConditionType#TOMBSTONE}.
         * @throws IllegalStateException In the case when the condition is already defined.
         */
        public SimpleCondition tombstone() {
            validate(type());

            type(ConditionType.TOMBSTONE);

            return new SimpleCondition(this);
        }
    }

    /**
     * Checks that condition is not defined yet. If the condition is already defined then the {@link IllegalStateException} will be thrown.
     *
     * @throws IllegalStateException In the case when the condition is already defined.
     */
    private static void validate(Enum<?> type) {
        if (type != null) {
            throw new IllegalStateException("Condition type " + type.name() + " is already defined.");
        }
    }

    /**
     * Defines a condition interface.
     */
    public interface InnerCondition {
        /**
         * Returns a key, which identifies an entry, which condition will be applied to.
         *
         * @return Key, which identifies an entry, which condition will be applied to.
         */
        byte[] key();

        ConditionType type();
    }

    /**
     * Defines an abstract condition with the key, which identifies an entry, which condition will be applied to.
     */
    private abstract static class AbstractCondition implements InnerCondition {
        /** Entry key. */
        private final byte[] key;

        /**
         * Condition type.
         */
        private ConditionType type;

        /**
         * Constructs a condition with the given entry key.
         *
         * @param key Key, which identifies an entry, which condition will be applied to.
         */
        private AbstractCondition(byte[] key) {
            this.key = key;
        }

        /**
         * Returns the key, which identifies an entry, which condition will be applied to.
         *
         * @return Key, which identifies an entry, which condition will be applied to.
         */
        @Override
        public byte[] key() {
            return key;
        }

        @Override
        public ConditionType type() {
            return type;
        }

        protected void type(ConditionType type) {
            this.type = type;
        }
    }
}
