package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.rpc.CliRequests;
import java.util.ArrayList;
import java.util.List;

public class AddLearnersRequestImpl implements CliRequests.AddLearnersRequest, CliRequests.AddLearnersRequest.Builder {
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

    @Override public CliRequests.AddLearnersRequest build() {
        return this;
    }
}
