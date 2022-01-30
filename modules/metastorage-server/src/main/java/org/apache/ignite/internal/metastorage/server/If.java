package org.apache.ignite.internal.metastorage.server;

public class If {
    private final Condition condition;
    private final ConditionBranch andThen;
    private final ConditionBranch orElse;
    
    public If(Condition condition, ConditionBranch andThen, ConditionBranch orElse) {
        this.condition = condition;
        this.andThen = andThen;
        this.orElse = orElse;
    }
    
    public Condition condition() {
        return condition;
    }
    
    public ConditionBranch andThen() {
        return andThen;
    }
    
    public ConditionBranch orElse() {
        return orElse;
    }
}
