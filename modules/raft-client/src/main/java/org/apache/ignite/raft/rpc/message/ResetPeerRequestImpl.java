package org.apache.ignite.raft.rpc.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.rpc.CliRequests;

class ResetPeerRequestImpl implements CliRequests.ResetPeerRequest, CliRequests.ResetPeerRequest.Builder {
    private String groupId;
    private String peerId;
    private List<String> oldPeersList = new ArrayList<>();
    private List<String> newPeersList = new ArrayList<>();

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public String getPeerId() {
        return peerId;
    }

    @Override public List<String> getOldPeersList() {
        return oldPeersList;
    }

    @Override public int getOldPeersCount() {
        return oldPeersList.size();
    }

    @Override public String getOldPeers(int index) {
        return oldPeersList.get(index);
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

    @Override public Builder setPeerId(String peerId) {
        this.peerId = peerId;

        return this;
    }

    @Override public Builder addNewPeers(String peerId) {
        newPeersList.add(peerId);

        return this;
    }

    @Override public CliRequests.ResetPeerRequest build() {
        return this;
    }
}
