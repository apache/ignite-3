package org.apache.ignite.raft.rpc;

public interface RaftGroupMessage extends Message {
    public String getGroupId();
}
