package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.RaftClientMessages;

class StatusResponseImpl implements RaftClientMessages.StatusResponse, RaftClientMessages.StatusResponse.Builder {
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

    @Override public RaftClientMessages.StatusResponse build() {
        return this;
    }
}
