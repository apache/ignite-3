package org.apache.ignite.internal.metastorage.server;

import org.apache.ignite.internal.metastorage.server.persistence.AbstractBinaryCondition;

public class AndCondition extends AbstractBinaryCondition {
    
    public AndCondition(Condition leftCondition, Condition rightCondition) {
        super(leftCondition, rightCondition);
    }
    
    @Override
    public boolean combine(boolean left, boolean right) {
        return left && right;
    }
}
