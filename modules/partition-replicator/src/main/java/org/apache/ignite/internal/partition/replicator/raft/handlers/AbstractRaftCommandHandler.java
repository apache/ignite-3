package org.apache.ignite.internal.partition.replicator.raft.handlers;

import java.io.Serializable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.raft.WriteCommand;
import org.jetbrains.annotations.Nullable;

/**
 * .
 *
 * @param <T> Type of the command.
 */
public abstract class AbstractRaftCommandHandler<T extends WriteCommand> {
    /**
     * Handles a command.
     *
     * @param command Write Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     * @param safeTimestamp Safe timestamp.
     */
    public final IgniteBiTuple<Serializable, Boolean> handle(
            WriteCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        return handleInternally((T) command, commandIndex, commandTerm, safeTimestamp);
    }

    protected abstract IgniteBiTuple<Serializable, Boolean> handleInternally(
            T cmd,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp);
}
