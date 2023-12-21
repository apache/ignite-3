package org.apache.ignite.internal.raft.service;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.raft.Command;

/**
 * Raft client that is only able to run commands on a Raft group. For a more potent interface, take a look at {@link RaftGroupService}.
 *
 * @see RaftGroupService
 */
public interface RaftCommandRunner {
    /**
     * Runs a command on a replication group leader.
     *
     * <p>Read commands always see up to date data.
     *
     * @param cmd The command.
     * @param <R> Execution result type.
     * @return A future with the execution result.
     */
    <R> CompletableFuture<R> run(Command cmd);
}
