package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientCommonMessages;

class TransferLeaderRequestImpl implements RaftClientCommonMessages.TransferLeaderRequest, RaftClientCommonMessages.TransferLeaderRequest.Builder {
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

    @Override public RaftClientCommonMessages.TransferLeaderRequest build() {
        return this;
    }
}
