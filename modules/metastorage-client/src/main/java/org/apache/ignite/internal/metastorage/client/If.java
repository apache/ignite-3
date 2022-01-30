package org.apache.ignite.internal.metastorage.client;

public class If {
    
    private final Condition condition;
    private final IfBranch andThen;
    private final IfBranch orElse;
    
    public If(Condition condition, IfBranch andThen, IfBranch orElse) {
        this.condition = condition;
        this.andThen = andThen;
        this.orElse = orElse;
    }
    
    public Condition condition() {
        return condition;
    }
    
    public IfBranch andThen() {
        return andThen;
    }
    
    public IfBranch orElse() {
        return orElse;
    }
}
