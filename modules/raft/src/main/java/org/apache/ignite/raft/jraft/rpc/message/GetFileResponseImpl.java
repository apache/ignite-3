package org.apache.ignite.raft.jraft.rpc.message;

import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.util.ByteString;

public class GetFileResponseImpl implements RpcRequests.GetFileResponse, RpcRequests.GetFileResponse.Builder {
    private boolean eof;
    private long readSize;
    private ByteString data;

    @Override public boolean getEof() {
        return eof;
    }

    @Override public long getReadSize() {
        return readSize;
    }

    @Override public RpcRequests.ErrorResponse getErrorResponse() {
        return null;
    }

    @Override public ByteString getData() {
        return data;
    }

    @Override public RpcRequests.GetFileResponse build() {
        return this;
    }

    @Override public Builder setReadSize(int read) {
        this.readSize = read;

        return this;
    }

    @Override public Builder setEof(boolean eof) {
        this.eof = eof;

        return this;
    }

    @Override public Builder setData(ByteString data) {
        this.data = data;

        return this;
    }
}
