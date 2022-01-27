package org.apache.ignite.internal.metastorage.server;

public abstract class AbstractCondition implements Condition {
    
    private final ConditionBranch andThen;
    
    private final ConditionBranch orElse;
    
    public AbstractCondition(ConditionBranch andThen, ConditionBranch orElse) {
        this.andThen = andThen;
        this.orElse = orElse;
    }
    
    @Override
    public ConditionBranch andThen() {
        return andThen;
    }
    
    @Override
    public ConditionBranch orElse() {
        return orElse;
    }
}
