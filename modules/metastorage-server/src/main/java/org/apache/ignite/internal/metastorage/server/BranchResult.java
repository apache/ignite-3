package org.apache.ignite.internal.metastorage.server;

public class BranchResult {
    
    private final boolean res;
    
    public BranchResult(boolean res) {
        this.res = res;
    }
    
    public boolean result() {
        return res;
    }
}
