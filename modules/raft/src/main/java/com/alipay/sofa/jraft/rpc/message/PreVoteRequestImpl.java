package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.rpc.RpcRequests;

class PreVoteRequestImpl implements RpcRequests.RequestVoteRequest, RpcRequests.RequestVoteRequest.Builder {
    private String groupId;
    private String serverId;
    private String peerId;
    private long term;
    private long lastLogTerm;
    private long lastLogIndex;
    private boolean preVote;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public String getServerId() {
        return serverId;
    }

    @Override public Builder setServerId(String serverId) {
        this.serverId = serverId;

        return this;
    }

    @Override public String getPeerId() {
        return peerId;
    }

    @Override public Builder setPeerId(String peerId) {
        this.peerId = peerId;

        return this;
    }

    @Override public long getTerm() {
        return term;
    }

    @Override public Builder setTerm(long term) {
        this.term = term;

        return this;
    }

    @Override public long getLastLogTerm() {
        return lastLogTerm;
    }

    @Override public Builder setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;

        return this;
    }

    @Override public long getLastLogIndex() {
        return lastLogIndex;
    }

    @Override public Builder setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;

        return this;
    }

    public boolean getPreVote() {
        return preVote;
    }

    @Override public Builder setPreVote(boolean preVote) {
        this.preVote = preVote;

        return this;
    }

    @Override public RpcRequests.RequestVoteRequest build() {
        return this;
    }
}
