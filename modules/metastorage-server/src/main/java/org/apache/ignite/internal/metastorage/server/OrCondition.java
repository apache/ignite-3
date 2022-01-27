package org.apache.ignite.internal.metastorage.server;

import org.apache.ignite.internal.metastorage.server.persistence.AbstractBinaryCondition;

public class OrCondition extends AbstractBinaryCondition {
    
    public OrCondition(Condition leftCondition, Condition rightCondition) {
        super(leftCondition, rightCondition);
    }
    
    @Override
    public boolean combine(boolean left, boolean right) {
        return left || right;
    }
}
