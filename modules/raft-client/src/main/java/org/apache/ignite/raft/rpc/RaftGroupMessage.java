package org.apache.ignite.raft.rpc;

/**
 * A message for a specific raft group.
 */
public interface RaftGroupMessage extends Message {
    public String getGroupId();
}
