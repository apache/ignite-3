package org.apache.ignite.internal.table.distributed.command;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.raft.client.WriteCommand;

public class PutCommand implements WriteCommand {
    BinaryRow row;

    public PutCommand(BinaryRow row) {
        this.row = row;
    }
}
