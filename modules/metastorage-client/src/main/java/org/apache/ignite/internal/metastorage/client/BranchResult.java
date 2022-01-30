package org.apache.ignite.internal.metastorage.client;

public class BranchResult {
    
    private final boolean result;
    
    public BranchResult(boolean result) {
        this.result = result;
    }
    
    public boolean result() {
        return result;
    }
}
