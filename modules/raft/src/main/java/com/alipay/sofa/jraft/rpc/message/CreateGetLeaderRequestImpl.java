package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.rpc.CliRequests;

public class CreateGetLeaderRequestImpl implements CliRequests.GetLeaderRequest, CliRequests.GetLeaderRequest.Builder {
    private String groupId;
    private String peerId;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public String getPeerId() {
        return peerId;
    }

    @Override public boolean hasPeerId() {
        return peerId != null;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setPeerId(String peerId) {
        this.peerId = peerId;

        return this;
    }

    @Override public CliRequests.GetLeaderRequest build() {
        return this;
    }
}
