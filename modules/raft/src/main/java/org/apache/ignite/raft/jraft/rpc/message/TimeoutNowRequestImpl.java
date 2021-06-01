package org.apache.ignite.raft.jraft.rpc.message;

import org.apache.ignite.raft.jraft.rpc.RpcRequests;

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

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TimeoutNowRequestImpl that = (TimeoutNowRequestImpl) o;

        if (term != that.term)
            return false;
        if (!groupId.equals(that.groupId))
            return false;
        if (!serverId.equals(that.serverId))
            return false;
        return peerId.equals(that.peerId);
    }

    @Override public int hashCode() {
        int result = groupId.hashCode();
        result = 31 * result + serverId.hashCode();
        result = 31 * result + peerId.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        return result;
    }
}
