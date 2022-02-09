package org.apache.ignite.internal.metastorage.common;

import java.io.Serializable;
import java.util.Collection;
import org.apache.ignite.internal.metastorage.common.command.OperationInfo;

public class UpdateInfo implements Serializable {
    private final Collection<OperationInfo> ops;
    
    private final StatementResultInfo result;
    
    public UpdateInfo(Collection<OperationInfo> ops, StatementResultInfo result) {
        this.ops = ops;
        this.result = result;
    }
    
    public Collection<OperationInfo> operations() {
        return ops;
    }
    
    public StatementResultInfo result() {
        return result;
    }
}
