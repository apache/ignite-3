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
 * Compound condition, which consists two other simple/compound conditions.
 */
public class CompoundCondition implements Condition {
    /** Left condition. */
    private final Condition leftCond;

    /** Right condition. */
    private final Condition rightCond;

    /** Compound condition type. */
    private final CompoundConditionType compoundCondType;

    /**
     * Constructs new compound condition.
     *
     * @param leftCond Left condition.
     * @param rightCond Right condition.
     * @param compoundCondType Compound condition type.
     */
    public CompoundCondition(Condition leftCond, Condition rightCond,
            CompoundConditionType compoundCondType) {
        this.leftCond = leftCond;
        this.rightCond = rightCond;
        this.compoundCondType = compoundCondType;
    }

    /**
     * Returns left condition.
     *
     * @return left condition.
     */
    public Condition leftCondition() {
        return leftCond;
    }

    /**
     * Returns right condition.
     *
     * @return right condition.
     */
    public Condition rightCondition() {
        return rightCond;
    }

    /**
     * Returns compound condition type.
     *
     * @return compound condition type.
     */
    public CompoundConditionType compoundConditionType() {
        return compoundCondType;
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
        return new CompoundCondition(leftCond, rightCond, CompoundConditionType.AND);
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
        return new CompoundCondition(leftCond, rightCond, CompoundConditionType.OR);
    }
}
