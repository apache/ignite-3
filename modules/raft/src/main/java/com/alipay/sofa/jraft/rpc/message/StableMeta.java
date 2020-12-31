package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.entity.LocalStorageOutter;

class StableMeta implements LocalStorageOutter.StablePBMeta, LocalStorageOutter.StablePBMeta.Builder {
    private long term;
    private String votedFor;

    @Override public long getTerm() {
        return term;
    }

    @Override public String getVotedfor() {
        return votedFor;
    }

    @Override public Builder setTerm(long term) {
        this.term = term;

        return this;
    }

    @Override public Builder setVotedfor(String votedFor) {
        this.votedFor = votedFor;

        return this;
    }

    @Override public LocalStorageOutter.StablePBMeta build() {
        return this;
    }
}
