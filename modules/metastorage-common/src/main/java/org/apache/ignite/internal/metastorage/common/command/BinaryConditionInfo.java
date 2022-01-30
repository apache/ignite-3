package org.apache.ignite.internal.metastorage.common.command;

import java.io.Serializable;

public class BinaryConditionInfo implements Serializable, ConditionInfo {
    
    private final ConditionInfo leftConditionInfo;
    private final ConditionInfo rightConditionInfo;
    private final BinaryConditionType type;
    
    public BinaryConditionInfo(ConditionInfo leftConditionInfo,
            ConditionInfo rightConditionInfo, BinaryConditionType type) {
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
    
    public BinaryConditionType type() {
        return type;
    }
    
}
