package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.entity.RaftOutter;
import java.util.ArrayList;
import java.util.List;

class SnapshotMetaImpl implements RaftOutter.SnapshotMeta, RaftOutter.SnapshotMeta.Builder {
    private long lastIncludedIndex;
    private long lastIncludedTerm;
    private List<String> peersList = new ArrayList<>();
    private List<String> oldPeersList = new ArrayList<>();
    private List<String> learnersList = new ArrayList<>();;
    private List<String> oldLearnersList = new ArrayList<>();;

    @Override public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    @Override public long getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    @Override public List<String> getPeersList() {
        return peersList;
    }

    @Override public int getPeersCount() {
        return peersList.size();
    }

    @Override public String getPeers(int index) {
        return peersList.get(index);
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

    @Override public List<String> getLearnersList() {
        return learnersList;
    }

    @Override public int getLearnersCount() {
        return learnersList.size();
    }

    @Override public String getLearners(int index) {
        return learnersList.get(index);
    }

    @Override public List<String> getOldLearnersList() {
        return oldLearnersList;
    }

    @Override public int getOldLearnersCount() {
        return oldLearnersList.size();
    }

    @Override public String getOldLearners(int index) {
        return oldLearnersList.get(index);
    }

    @Override public RaftOutter.SnapshotMeta build() {
        return this;
    }

    @Override public Builder setLastIncludedIndex(long lastAppliedIndex) {
        this.lastIncludedIndex = lastAppliedIndex;

        return this;
    }

    @Override public Builder setLastIncludedTerm(long lastAppliedTerm) {
        this.lastIncludedTerm = lastAppliedTerm;

        return this;
    }

    @Override public Builder addPeers(String peerId) {
        peersList.add(peerId);

        return this;
    }

    @Override public Builder addLearners(String learnerId) {
        learnersList.add(learnerId);

        return this;
    }

    @Override public Builder addOldPeers(String oldPeerId) {
        oldPeersList.add(oldPeerId);

        return this;
    }

    @Override public Builder addOldLearners(String oldLearnerId) {
        oldLearnersList.add(oldLearnerId);

        return this;
    }
}
