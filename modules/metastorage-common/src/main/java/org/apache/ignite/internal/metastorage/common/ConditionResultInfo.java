package org.apache.ignite.internal.metastorage.common;

public class ConditionResultInfo {
    private final boolean res;
    
    public ConditionResultInfo(boolean res) {
        this.res = res;
    }
    
    public boolean result() {
        return res;
    }
    
}
