package org.apache.ignite.raft.rpc.message;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.rpc.CliRequests;

class ResetLearnersRequestImpl implements CliRequests.ResetLearnersRequest, CliRequests.ResetLearnersRequest.Builder {
    private String groupId;
    private String leaderId;
    private List<String> learnersList = new ArrayList<>();

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public String getLeaderId() {
        return leaderId;
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

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder setLeaderId(String leaderId) {
        this.leaderId = leaderId;

        return this;
    }

    @Override public Builder addLearners(String learnerId) {
        learnersList.add(learnerId);

        return this;
    }

    @Override public CliRequests.ResetLearnersRequest build() {
        return this;
    }
}
