package org.apache.ignite.internal.metastorage.server;

import java.util.Collection;

public class Update {
    
    private final Collection<Operation> ops;
    
    private final ConditionResult result;
    
    public Update(Collection<Operation> ops, ConditionResult result) {
        this.ops = ops;
        this.result = result;
    }
    
    public Collection<Operation> operations() {
        return ops;
    }
    
    public ConditionResult result() {
        return result;
    }
}
