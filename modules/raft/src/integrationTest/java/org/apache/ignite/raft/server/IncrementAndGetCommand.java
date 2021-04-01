package org.apache.ignite.raft.server;

import org.apache.ignite.raft.client.WriteCommand;

public class IncrementAndGetCommand implements WriteCommand {
    int delta;
}
