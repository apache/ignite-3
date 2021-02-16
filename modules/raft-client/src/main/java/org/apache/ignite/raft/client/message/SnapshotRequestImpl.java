package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.client.RaftClientCommonMessages;

class SnapshotRequestImpl implements RaftClientCommonMessages.SnapshotRequest, RaftClientCommonMessages.SnapshotRequest.Builder {
    private String groupId;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public RaftClientCommonMessages.SnapshotRequest build() {
        return this;
    }
}
