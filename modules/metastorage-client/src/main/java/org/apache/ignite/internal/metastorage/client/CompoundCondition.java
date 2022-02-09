package org.apache.ignite.internal.metastorage.client;

import org.apache.ignite.internal.metastorage.common.command.CompoundConditionType;

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
