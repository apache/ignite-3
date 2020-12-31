package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.rpc.RpcRequests;

class TimeoutNowRequestImpl implements RpcRequests.TimeoutNowRequest, RpcRequests.TimeoutNowRequest.Builder {
    private String groupId;
    private String serverId;
    private String peerId;
    private long term;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public String getServerId() {
        return serverId;
    }

    @Override public String getPeerId() {
        return peerId;
    }

    @Override public long getTerm() {
        return term;
    }

    @Override public RpcRequests.TimeoutNowRequest build() {
        return this;
    }

    @Override public Builder setTerm(long term) {
        this.term = term;

        return this;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setServerId(String serverId) {
        this.serverId = serverId;

        return this;
    }

    @Override public Builder setPeerId(String peerId) {
        this.peerId = peerId;

        return this;
    }
}
