package org.apache.ignite.raft.jraft.rpc.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.jraft.rpc.CliRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;

public class ChangePeersResponseImpl implements CliRequests.ChangePeersResponse, CliRequests.ChangePeersResponse.Builder {
    private List<String> oldPeersList = new ArrayList<>();
    private List<String> newPeersList = new ArrayList<>();

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

    @Override public RpcRequests.ErrorResponse getErrorResponse() {
        return null;
    }

    @Override public Builder addOldPeers(String oldPeerId) {
        oldPeersList.add(oldPeerId);

        return this;
    }

    @Override public Builder addNewPeers(String newPeerId) {
        newPeersList.add(newPeerId);

        return this;
    }

    @Override public CliRequests.ChangePeersResponse build() {
        return this;
    }
}
