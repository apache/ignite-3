package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientCommonMessages;

class RemovePeerRequestImpl implements RaftClientCommonMessages.RemovePeerRequest, RaftClientCommonMessages.RemovePeerRequest.Builder {
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

    @Override public RaftClientCommonMessages.RemovePeerRequest build() {
        return this;
    }
}
