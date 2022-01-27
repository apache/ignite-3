package org.apache.ignite.internal.metastorage.server;

public class ConditionResult {
    
    private final boolean res;
    
    public ConditionResult(boolean res) {
        this.res = res;
    }
    
    public boolean result() {
        return res;
    }
}
