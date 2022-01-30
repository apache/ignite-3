package org.apache.ignite.internal.metastorage.client;

import org.apache.ignite.internal.metastorage.common.command.BinaryConditionType;

public class BinaryCondition extends AbstractCondition {
    
    private final Condition leftCondition;
    private final Condition rightCondition;
    private final BinaryConditionType binaryConditionType;
    
    public BinaryCondition(Condition leftCondition, Condition rightCondition,
            BinaryConditionType binaryConditionType) {
        this.leftCondition = leftCondition;
        this.rightCondition = rightCondition;
        this.binaryConditionType = binaryConditionType;
    }
    
    public Condition leftCondition() {
        return leftCondition;
    }
    
    public Condition rightCondition() {
        return rightCondition;
    }
    
    public BinaryConditionType binaryConditionType() {
        return binaryConditionType;
    }
}
