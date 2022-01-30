package org.apache.ignite.internal.metastorage.common.command;

import org.apache.ignite.raft.client.Command;

public class MultiInvokeCommand implements Command {
    
    private final IfInfo _if;
    
    public MultiInvokeCommand(IfInfo _if) {
        this._if = _if;
    }
    
    public IfInfo _if() {
        return _if;
    }
}
