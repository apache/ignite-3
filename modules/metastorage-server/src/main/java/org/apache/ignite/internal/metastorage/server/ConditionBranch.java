package org.apache.ignite.internal.metastorage.server;

public class ConditionBranch {
    private final Condition condition;
    private final Update update;
    
    
    public ConditionBranch(Condition condition) {
        this.condition = condition;
        this.update = null;
    }
    
    public ConditionBranch(Update update) {
        this.update = update;
        this.condition = null;
    }
    
    public boolean isTerminal() {
        return update != null;
    }
    
    public Condition condition() {
        return condition;
    }
    
    public Update update() {
        return update;
    }
}
