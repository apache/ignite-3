package org.apache.ignite.internal.metastorage.client;

import org.apache.ignite.internal.metastorage.common.command.BinaryConditionType;

public class AbstractCondition implements Condition {
    @Override
    public Condition and(Condition condition) {
        return new BinaryCondition(this, condition, BinaryConditionType.AND);
    }
    
    @Override
    public Condition or(Condition condition) {
        return new BinaryCondition(this, condition, BinaryConditionType.OR);
    }
}
