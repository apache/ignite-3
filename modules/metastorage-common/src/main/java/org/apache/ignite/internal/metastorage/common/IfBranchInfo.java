package org.apache.ignite.internal.metastorage.common;

import org.apache.ignite.internal.metastorage.common.command.IfInfo;

public class IfBranchInfo {
    private final IfInfo _if;
    private final UpdateInfo update;
    
    public IfBranchInfo(IfInfo _if) {
        this._if = _if;
        this.update = null;
    }
    
    public IfBranchInfo(UpdateInfo update) {
        this.update = update;
        this._if = null;
    }
    
    public boolean isTerminal() {
        return update != null;
    }
    
    public IfInfo _if() {
        return _if;
    }
    
    public UpdateInfo update() {
        return update;
    }
}
