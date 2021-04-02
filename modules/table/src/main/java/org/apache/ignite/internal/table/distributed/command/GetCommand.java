package org.apache.ignite.internal.table.distributed.command;

import org.apache.ignite.internal.table.TableRow;
import org.apache.ignite.raft.client.ReadCommand;

public class GetCommand implements ReadCommand {
    TableRow keyRow;

    public GetCommand(TableRow keyRow) {
        this.keyRow = keyRow;
    }
}
