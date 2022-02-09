package org.apache.ignite.internal.metastorage.common.command;

import org.apache.ignite.raft.client.Command;

public class MultiInvokeCommand implements Command {

    private final IfInfo iif;

    public MultiInvokeCommand(IfInfo iif) {
        this.iif = iif;
    }

    public IfInfo iif() {
        return iif;
    }
}
