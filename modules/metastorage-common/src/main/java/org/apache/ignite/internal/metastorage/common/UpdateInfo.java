package org.apache.ignite.internal.metastorage.common;

import java.util.Collection;
import org.apache.ignite.internal.metastorage.common.command.OperationInfo;

public class UpdateInfo {
    private final Collection<OperationInfo> ops;
    
    private final BranchResultInfo result;
    
    public UpdateInfo(Collection<OperationInfo> ops, BranchResultInfo result) {
        this.ops = ops;
        this.result = result;
    }
    
    public Collection<OperationInfo> operations() {
        return ops;
    }
    
    public BranchResultInfo result() {
        return result;
    }
}
