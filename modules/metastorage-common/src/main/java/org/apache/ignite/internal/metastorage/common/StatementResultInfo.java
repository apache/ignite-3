package org.apache.ignite.internal.metastorage.common;

import java.io.Serializable;

public class StatementResultInfo implements Serializable {
    private final boolean res;
    
    public StatementResultInfo(boolean res) {
        this.res = res;
    }
    
    public boolean result() {
        return res;
    }
    
}
