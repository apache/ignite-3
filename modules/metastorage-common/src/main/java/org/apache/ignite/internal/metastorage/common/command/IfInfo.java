package org.apache.ignite.internal.metastorage.common.command;

import java.io.Serializable;
import org.apache.ignite.internal.metastorage.common.StatementInfo;

public class IfInfo implements Serializable {
    private final ConditionInfo cond;
    private final StatementInfo andThen;
    private final StatementInfo orElse;
    
    public IfInfo(ConditionInfo cond, StatementInfo andThen,
            StatementInfo orElse) {
        this.cond = cond;
        this.andThen = andThen;
        this.orElse = orElse;
    }
    
    public ConditionInfo cond() {
        return cond;
    }
    
    public StatementInfo andThen() {
        return andThen;
    }
    
    public StatementInfo orElse() {
        return orElse;
    }
}
