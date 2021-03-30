package org.apache.ignite.raft.jraft.rpc.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.jraft.rpc.CliRequests;

public class ChangePeerRequestImpl implements CliRequests.ChangePeersRequest, CliRequests.ChangePeersRequest.Builder {
    private String groupId;
    private String leaderId;
    private List<String> newPeersList = new ArrayList<>();

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public String getLeaderId() {
        return leaderId;
    }

    @Override public List<String> getNewPeersList() {
        return newPeersList;
    }

    @Override public int getNewPeersCount() {
        return newPeersList.size();
    }

    @Override public String getNewPeers(int index) {
        return newPeersList.get(index);
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setLeaderId(String leaderId) {
        this.leaderId = leaderId;

        return this;
    }

    @Override public Builder addNewPeers(String peerId) {
        newPeersList.add(peerId);

        return this;
    }

    @Override public CliRequests.ChangePeersRequest build() {
        return this;
    }
}
