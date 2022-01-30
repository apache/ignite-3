package org.apache.ignite.internal.metastorage.client;

import java.util.Collection;

public class Update {
    
    private final Collection<Operation> ops;
    
    private final BranchResult result;
    
    public Update(Collection<Operation> ops, BranchResult result) {
        this.ops = ops;
        this.result = result;
    }
    
    public Collection<Operation> operations() {
        return ops;
    }
    
    public BranchResult result() {
        return result;
    }
}
