package org.apache.ignite.internal.table.distributed.command;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.raft.client.ReadCommand;

public class GetCommand implements ReadCommand {
    BinaryRow keyRow;

    public GetCommand(BinaryRow keyRow) {
        this.keyRow = keyRow;
    }
}
