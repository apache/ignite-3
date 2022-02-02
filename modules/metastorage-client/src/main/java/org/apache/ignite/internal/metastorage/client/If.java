package org.apache.ignite.internal.metastorage.client;

public class If {
    
    private final Condition condition;
    private IfBranch andThen;
    private IfBranch orElse;
    
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
    
    public static If _if(Condition condition, If andThen, If orElse) {
        return new If(condition, new IfBranch(andThen), new IfBranch(orElse));
    }
    
    public static If _if(Condition condition, If andThen, Update orElse) {
        return new If(condition, new IfBranch(andThen), new IfBranch(orElse));
    }
    
    public static If _if(Condition condition, Update andThen, If orElse) {
        return new If(condition, new IfBranch(andThen), new IfBranch(orElse));
    }
    
    public static If _if(Condition condition, Update andThen, Update orElse) {
        return new If(condition, new IfBranch(andThen), new IfBranch(orElse));
    }
    
}

