package org.apache.ignite.raft.rpc.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.rpc.CliRequests;

class LearnersOpResponseImpl implements CliRequests.LearnersOpResponse, CliRequests.LearnersOpResponse.Builder {
    private List<String> oldLearnersList = new ArrayList<>();
    private List<String> newLearnersList = new ArrayList<>();

    @Override public List<String> getOldLearnersList() {
        return oldLearnersList;
    }

    @Override public int getOldLearnersCount() {
        return oldLearnersList.size();
    }

    @Override public String getOldLearners(int index) {
        return oldLearnersList.get(index);
    }

    @Override public List<String> getNewLearnersList() {
        return newLearnersList;
    }

    @Override public int getNewLearnersCount() {
        return newLearnersList.size();
    }

    @Override public String getNewLearners(int index) {
        return newLearnersList.get(index);
    }

    @Override public Builder addOldLearners(String oldLearnersId) {
        oldLearnersList.add(oldLearnersId);

        return this;
    }

    @Override public Builder addNewLearners(String newLearnersId) {
        newLearnersList.add(newLearnersId);

        return this;
    }

    @Override public CliRequests.LearnersOpResponse build() {
        return this;
    }
}
