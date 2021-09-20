package org.apache.ignite.internal.table.distributed.command;

import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.raft.client.WriteCommand;

public class FinishTxCommand implements WriteCommand {
    /** The timestamp. */
    private final Timestamp timestamp;

    /** Commit or rollback state. */
    private boolean finish;

    /**
     * @param timestamp The timestamp.
     * @param finish Commit or rollback state {@code True} to commit.
     */
    public FinishTxCommand(Timestamp timestamp, boolean finish) {
        this.timestamp = timestamp;
        this.finish = finish;
    }

    /**
     * @return The timestamp.
     */
    public Timestamp timestamp() {
        return timestamp;
    }

    /**
     * @return Commit or rollback state.
     */
    public boolean finish() {
        return finish;
    }
}
