package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter;
import com.alipay.sofa.jraft.entity.LocalStorageOutter;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.rpc.CliRequests;
import com.alipay.sofa.jraft.rpc.MessageBuilderFactory;
import com.alipay.sofa.jraft.rpc.RpcRequests;

public class DefaultMessageBuilderFactory implements MessageBuilderFactory {
    @Override public CliRequests.AddPeerRequest.Builder createAddPeerRequest() {
        return new AddPeerRequestImpl();
    }

    @Override public LocalFileMetaOutter.LocalFileMeta.Builder createLocalFileMeta() {
        return new LocalFileMetaImpl();
    }

    @Override public RpcRequests.PingRequest.Builder createPingRequest() {
        return new PingRequestImpl();
    }

    @Override public RpcRequests.RequestVoteRequest.Builder createVoteRequest() {
        return new PreVoteRequestImpl();
    }

    @Override public RpcRequests.RequestVoteResponse.Builder createVoteResponse() {
        return new RequestVoteResponseImpl();
    }

    @Override public RpcRequests.ErrorResponse.Builder createErrorResponse() {
        return new ErrorResponseImpl();
    }

    @Override public LocalStorageOutter.StablePBMeta.Builder createStableMeta() {
        return new StableMeta();
    }

    @Override public RpcRequests.AppendEntriesRequest.Builder createAppendEntriesRequest() {
        return new AppendEntriesRequestImpl();
    }

    @Override public RpcRequests.AppendEntriesResponse.Builder createAppendEntriesResponse() {
        return new AppendEntriesResponseImpl();
    }

    @Override public RaftOutter.EntryMeta.Builder createEntryMeta() {
        return new EntryMetaImpl();
    }

    @Override public RpcRequests.TimeoutNowRequest.Builder createTimeoutNowRequest() {
        return new TimeoutNowRequestImpl();
    }

    @Override public RpcRequests.TimeoutNowResponse.Builder createTimeoutNowResponse() {
        return new TimeoutNowResponseImpl();
    }

    @Override public RpcRequests.ReadIndexRequest.Builder createReadIndexRequest() {
        return new ReadIndexRequestImpl();
    }

    @Override public RpcRequests.ReadIndexResponse.Builder createReadIndexResponse() {
        return new ReadIndexResponseImpl();
    }
}
