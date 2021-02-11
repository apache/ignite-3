package org.apache.ignite.raft.rpc.message;

import org.apache.ignite.raft.rpc.CliRequests;

class AddPeerRequestImpl implements CliRequests.AddPeerRequest, CliRequests.AddPeerRequest.Builder {
    private String groupId;
    private String leaderId;
    private String peerId;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public String getLeaderId() {
        return leaderId;
    }

    @Override public String getPeerId() {
        return peerId;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setLeaderId(String leaderId) {
        this.leaderId = leaderId;

        return this;
    }

    @Override public Builder setPeerId(String peerId) {
        this.peerId = peerId;

        return this;
    }

    @Override public CliRequests.AddPeerRequest build() {
        return this;
    }
}
