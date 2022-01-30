package org.apache.ignite.internal.metastorage.client;

import org.apache.ignite.internal.metastorage.common.command.BinaryConditionType;

public class BinaryCondition implements Condition {
    
    private final UnaryCondition leftCondition;
    private final UnaryCondition rightCondition;
    private final BinaryConditionType binaryConditionType;
    
    public BinaryCondition(UnaryCondition leftCondition, UnaryCondition rightCondition,
            BinaryConditionType binaryConditionType) {
        this.leftCondition = leftCondition;
        this.rightCondition = rightCondition;
        this.binaryConditionType = binaryConditionType;
    }
    
    public UnaryCondition leftCondition() {
        return leftCondition;
    }
    
    public UnaryCondition rightCondition() {
        return rightCondition;
    }
    
    public BinaryConditionType binaryConditionType() {
        return binaryConditionType;
    }
}
