package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientMessages;

class TransferLeaderRequestImpl implements RaftClientMessages.TransferLeaderRequest, RaftClientMessages.TransferLeaderRequest.Builder {
    private String groupId;
    private PeerId peerId;

    public String getGroupId() {
        return groupId;
    }

    @Override public PeerId getPeerId() {
        return peerId;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public RaftClientMessages.TransferLeaderRequest build() {
        return this;
    }
}
