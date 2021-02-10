package org.apache.ignite.raft.rpc.message;

import org.apache.ignite.raft.rpc.CliRequests;

class SnapshotRequestImpl implements CliRequests.SnapshotRequest, CliRequests.SnapshotRequest.Builder {
    private String groupId;
    private String peerId;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public String getPeerId() {
        return peerId;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setPeerId(String peerId) {
        this.peerId = peerId;

        return this;
    }

    @Override public CliRequests.SnapshotRequest build() {
        return this;
    }
}
