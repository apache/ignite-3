package org.apache.ignite.raft.jraft.rpc.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.jraft.rpc.CliRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;

public class GetPeersResponseImpl implements CliRequests.GetPeersResponse, CliRequests.GetPeersResponse.Builder {
    private List<String> peersList = new ArrayList<>();
    private List<String> learnersList = new ArrayList<>();

    @Override public List<String> getPeersList() {
        return peersList;
    }

    @Override public int getPeersCount() {
        return peersList.size();
    }

    @Override public String getPeers(int index) {
        return peersList.get(index);
    }

    @Override public List<String> getLearnersList() {
        return learnersList;
    }

    @Override public int getLearnersCount() {
        return learnersList.size();
    }

    @Override public String getLearners(int index) {
        return learnersList.get(index);
    }

    @Override public RpcRequests.ErrorResponse getErrorResponse() {
        return null;
    }

    @Override public Builder addPeers(String peerId) {
        peersList.add(peerId);

        return this;
    }

    @Override public Builder addLearners(String learnerId) {
        learnersList.add(learnerId);

        return this;
    }

    @Override public CliRequests.GetPeersResponse build() {
        return this;
    }
}
