package org.apache.ignite.internal.metastorage.server;

public class OrCondition extends AbstractCompoundCondition {

    public OrCondition(Condition leftCondition, Condition rightCondition) {
        super(leftCondition, rightCondition);
    }

    @Override
    protected boolean combine(boolean left, boolean right) {
        return left || right;
    }
}
