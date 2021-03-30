package org.apache.ignite.raft.jraft.rpc.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.util.ByteString;

class ReadIndexRequestImpl implements RpcRequests.ReadIndexRequest, RpcRequests.ReadIndexRequest.Builder {
    private String groupId;
    private String serverId;
    private List<ByteString> entriesList = new ArrayList<>();
    private String peerId;

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public String getServerId() {
        return serverId;
    }

    @Override public List<ByteString> getEntriesList() {
        return entriesList;
    }

    @Override public int getEntriesCount() {
        return entriesList.size();
    }

    @Override public ByteString getEntries(int index) {
        return entriesList.get(index);
    }

    @Override public String getPeerId() {
        return peerId;
    }

    @Override public RpcRequests.ReadIndexRequest build() {
        return this;
    }

    @Override public Builder mergeFrom(RpcRequests.ReadIndexRequest request) {
        setGroupId(request.getGroupId());
        setServerId(request.getServerId());
        setPeerId(request.getPeerId());
        for (ByteString data : request.getEntriesList()) {
            addEntries(data);
        }

        return this;
    }

    @Override public Builder setPeerId(String peerId) {
        this.peerId = peerId;

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

    @Override public Builder addEntries(ByteString data) {
        entriesList.add(data);

        return this;
    }
}
