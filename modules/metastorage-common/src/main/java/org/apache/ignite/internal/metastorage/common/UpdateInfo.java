package org.apache.ignite.internal.metastorage.common;

import java.util.Collection;
import org.apache.ignite.internal.metastorage.common.command.OperationInfo;

public class UpdateInfo {
    private final Collection<OperationInfo> ops;
    
    private final ConditionResultInfo result;
    
    public UpdateInfo(Collection<OperationInfo> ops, ConditionResultInfo result) {
        this.ops = ops;
        this.result = result;
    }
    
    public Collection<OperationInfo> operations() {
        return ops;
    }
    
    public ConditionResultInfo result() {
        return result;
    }
}
