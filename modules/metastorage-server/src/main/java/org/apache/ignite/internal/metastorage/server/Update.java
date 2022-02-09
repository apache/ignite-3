package org.apache.ignite.internal.metastorage.server;

import java.util.Collection;

public class Update {
    
    private final Collection<Operation> ops;
    
    private final StatementResult result;
    
    public Update(Collection<Operation> ops, StatementResult result) {
        this.ops = ops;
        this.result = result;
    }
    
    public Collection<Operation> operations() {
        return ops;
    }
    
    public StatementResult result() {
        return result;
    }
}
