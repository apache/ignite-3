package org.apache.ignite.raft.client;

/**
 * A read command which can read stale data. Can be executed on an arbitrary peer.
 */
public interface WeakReadCommand extends Command {
}
