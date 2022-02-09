package org.apache.ignite.internal.metastorage.common.command;

public class CompoundConditionInfo implements ConditionInfo {

    private final ConditionInfo leftConditionInfo;
    private final ConditionInfo rightConditionInfo;
    private final CompoundConditionType type;

    public CompoundConditionInfo(ConditionInfo leftConditionInfo,
            ConditionInfo rightConditionInfo, CompoundConditionType type) {
        this.leftConditionInfo = leftConditionInfo;
        this.rightConditionInfo = rightConditionInfo;
        this.type = type;
    }

    public ConditionInfo leftConditionInfo() {
        return leftConditionInfo;
    }

    public ConditionInfo rightConditionInfo() {
        return rightConditionInfo;
    }

    public CompoundConditionType type() {
        return type;
    }

}
