package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.rpc.CliRequests;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import java.util.ArrayList;
import java.util.List;

public class AddPeerResponseImpl implements CliRequests.AddPeerResponse, CliRequests.AddPeerResponse.Builder {
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

    @Override public Builder addOldPeers(String oldPeersId) {
        oldPeersList.add(oldPeersId);

        return this;
    }

    @Override public Builder addNewPeers(String newPeersId) {
        newPeersList.add(newPeersId);

        return this;
    }

    @Override public CliRequests.AddPeerResponse build() {
        return this;
    }
}
