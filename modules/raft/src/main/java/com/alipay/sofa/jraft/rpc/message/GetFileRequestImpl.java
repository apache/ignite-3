package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.rpc.RpcRequests;

public class GetFileRequestImpl implements RpcRequests.GetFileRequest, RpcRequests.GetFileRequest.Builder {
    private long readerId;
    private String filename;
    private long count;
    private long offset;
    private boolean readPartly;


    @Override public long getReaderId() {
        return readerId;
    }

    @Override public String getFilename() {
        return filename;
    }

    @Override public long getCount() {
        return count;
    }

    @Override public long getOffset() {
        return offset;
    }

    @Override public boolean getReadPartly() {
        return readPartly;
    }

    @Override public RpcRequests.GetFileRequest build() {
        return this;
    }

    @Override public Builder setCount(long cnt) {
        this.count = cnt;

        return this;
    }

    @Override public Builder setOffset(long offset) {
        this.offset = offset;

        return this;
    }

    @Override public Builder setReadPartly(boolean readPartly) {
        this.readPartly = readPartly;

        return this;
    }

    @Override public Builder setFilename(String fileName) {
        this.filename = fileName;

        return this;
    }

    @Override public Builder setReaderId(long readerId) {
        this.readerId = readerId;

        return this;
    }
}
