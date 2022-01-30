package org.apache.ignite.internal.metastorage.common;

import java.io.Serializable;

public class BranchResultInfo implements Serializable {
    private final boolean res;
    
    public BranchResultInfo(boolean res) {
        this.res = res;
    }
    
    public boolean result() {
        return res;
    }
    
}
