package org.apache.ignite.internal.metastorage.server;

public class AndCondition extends AbstractBinaryCondition {
    
    public AndCondition(Condition leftCondition, Condition rightCondition) {
        super(leftCondition, rightCondition);
    }
    
    @Override
    protected boolean combine(boolean left, boolean right) {
        return left && right;
    }
}
