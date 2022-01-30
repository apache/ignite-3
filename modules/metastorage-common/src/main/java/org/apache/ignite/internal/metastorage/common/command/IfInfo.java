package org.apache.ignite.internal.metastorage.common.command;

import org.apache.ignite.internal.metastorage.common.ConditionBranchInfo;

public class IfInfo {
    private final ConditionInfoMarker cond;
    private final ConditionBranchInfo andThen;
    private final ConditionBranchInfo orElse;
    
    public IfInfo(ConditionInfoMarker cond, ConditionBranchInfo andThen,
            ConditionBranchInfo orElse) {
        this.cond = cond;
        this.andThen = andThen;
        this.orElse = orElse;
    }
    
    public ConditionInfoMarker cond() {
        return cond;
    }
    
    public ConditionBranchInfo andThen() {
        return andThen;
    }
    
    public ConditionBranchInfo orElse() {
        return orElse;
    }
}
