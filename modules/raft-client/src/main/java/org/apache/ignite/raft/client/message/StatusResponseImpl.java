package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientCommonMessages;

class StatusResponseImpl implements RaftClientCommonMessages.StatusResponse, RaftClientCommonMessages.StatusResponse.Builder {
    private int errorCode;
    private String errorMsg = "";

    @Override public int getStatusCode() {
        return errorCode;
    }

    @Override public Builder setStatusCode(int errorCode) {
        this.errorCode = errorCode;

        return this;
    }

    @Override public String getStatusMsg() {
        return errorMsg;
    }

    @Override public Builder setStatusMsg(String errorMsg) {
        this.errorMsg = errorMsg;

        return this;
    }

    @Override public RaftClientCommonMessages.StatusResponse build() {
        return this;
    }
}
