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

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GetFileRequestImpl that = (GetFileRequestImpl) o;

        if (readerId != that.readerId) return false;
        if (count != that.count) return false;
        if (offset != that.offset) return false;
        if (readPartly != that.readPartly) return false;
        return filename.equals(that.filename);
    }

    @Override public int hashCode() {
        int result = (int) (readerId ^ (readerId >>> 32));
        result = 31 * result + filename.hashCode();
        result = 31 * result + (int) (count ^ (count >>> 32));
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + (readPartly ? 1 : 0);
        return result;
    }
}
