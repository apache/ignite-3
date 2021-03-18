package org.apache.ignite.raft.client;

/**
 * A read command with linearizable guaranties (never see stale data). Executed on a leader.
 */
public interface ReadCommand extends Command {
}
