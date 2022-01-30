package org.apache.ignite.internal.metastorage.common.command;

import java.io.Serializable;
import org.apache.ignite.internal.metastorage.common.IfBranchInfo;

public class IfInfo implements Serializable {
    private final ConditionInfo cond;
    private final IfBranchInfo andThen;
    private final IfBranchInfo orElse;
    
    public IfInfo(ConditionInfo cond, IfBranchInfo andThen,
            IfBranchInfo orElse) {
        this.cond = cond;
        this.andThen = andThen;
        this.orElse = orElse;
    }
    
    public ConditionInfo cond() {
        return cond;
    }
    
    public IfBranchInfo andThen() {
        return andThen;
    }
    
    public IfBranchInfo orElse() {
        return orElse;
    }
}
