package org.apache.ignite.internal.metastorage.server;

public class If {
    private final Condition condition;
    private final Statement andThen;
    private final Statement orElse;
    
    public If(Condition condition, Statement andThen, Statement orElse) {
        this.condition = condition;
        this.andThen = andThen;
        this.orElse = orElse;
    }
    
    public Condition condition() {
        return condition;
    }
    
    public Statement andThen() {
        return andThen;
    }
    
    public Statement orElse() {
        return orElse;
    }
}
