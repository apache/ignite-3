package org.apache.ignite.internal.metastorage.common;

public class BranchResultInfo {
    private final boolean res;
    
    public BranchResultInfo(boolean res) {
        this.res = res;
    }
    
    public boolean result() {
        return res;
    }
    
}
