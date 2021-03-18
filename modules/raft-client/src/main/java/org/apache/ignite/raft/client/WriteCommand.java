package org.apache.ignite.raft.client;

/**
 * A write command which goes through the replicated log. Executed on a leader.
 */
public interface WriteCommand extends Command {
}
