package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientMessages;

class RemovePeerRequestImpl implements RaftClientMessages.RemovePeerRequest, RaftClientMessages.RemovePeerRequest.Builder {
    private String groupId;
    private PeerId peerId;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public PeerId getPeerId() {
        return peerId;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setPeerId(PeerId peerId) {
        this.peerId = peerId;

        return this;
    }

    @Override public RaftClientMessages.RemovePeerRequest build() {
        return this;
    }
}
