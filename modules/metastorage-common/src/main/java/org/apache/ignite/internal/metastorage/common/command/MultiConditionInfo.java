package org.apache.ignite.internal.metastorage.common.command;

import java.io.Serializable;
import org.apache.ignite.internal.metastorage.common.ConditionBranchInfo;

public class MultiConditionInfo implements Serializable, ConditionInfoMarker {
    
    private final ConditionInfoMarker leftConditionInfo;
    private final ConditionInfoMarker rightConditionInfo;
    private final BinaryConditionType type;
    
    public MultiConditionInfo(ConditionInfoMarker leftConditionInfo,
            ConditionInfoMarker rightConditionInfo, BinaryConditionType type, ConditionBranchInfo andThen, ConditionBranchInfo orElse) {
        this.leftConditionInfo = leftConditionInfo;
        this.rightConditionInfo = rightConditionInfo;
        this.type = type;
    }
    
    public ConditionInfoMarker leftConditionInfo() {
        return leftConditionInfo;
    }
    
    public ConditionInfoMarker rightConditionInfo() {
        return rightConditionInfo;
    }
    
    public BinaryConditionType type() {
        return type;
    }
    
}
