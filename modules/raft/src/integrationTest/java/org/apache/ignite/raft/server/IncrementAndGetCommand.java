package org.apache.ignite.raft.server;

import org.apache.ignite.raft.client.WriteCommand;

public class IncrementAndGetCommand implements WriteCommand {
    private final int delta;

    public IncrementAndGetCommand(int delta) {
        this.delta = delta;
    }

    public int delta() {
        return delta;
    }
}
