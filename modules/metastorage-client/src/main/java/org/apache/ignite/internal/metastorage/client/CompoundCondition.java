package org.apache.ignite.internal.metastorage.client;

import org.apache.ignite.internal.metastorage.common.command.CompoundConditionType;

public class CompoundCondition implements Condition {

    private final Condition leftCondition;
    private final Condition rightCondition;
    private final CompoundConditionType compoundConditionType;

    public CompoundCondition(Condition leftCondition, Condition rightCondition,
            CompoundConditionType compoundConditionType) {
        this.leftCondition = leftCondition;
        this.rightCondition = rightCondition;
        this.compoundConditionType = compoundConditionType;
    }

    public Condition leftCondition() {
        return leftCondition;
    }

    public Condition rightCondition() {
        return rightCondition;
    }

    public CompoundConditionType binaryConditionType() {
        return compoundConditionType;
    }

    public static CompoundCondition and(Condition left, Condition right) {
        return new CompoundCondition(left, right, CompoundConditionType.AND);
    }

    public static CompoundCondition or(Condition left, Condition right) {
        return new CompoundCondition(left, right, CompoundConditionType.OR);
    }
}
