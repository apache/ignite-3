package com.alipay.sofa.jraft.rpc;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter;
import com.alipay.sofa.jraft.entity.LocalStorageOutter;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.rpc.message.DefaultMessageBuilderFactory;

// TODO asch use JRaftServiceLoader ?
public interface MessageBuilderFactory {
    public static MessageBuilderFactory DEFAULT = new DefaultMessageBuilderFactory();

    CliRequests.AddPeerRequest.Builder createAddPeerRequest();

    LocalFileMetaOutter.LocalFileMeta.Builder createLocalFileMeta();

    RpcRequests.PingRequest.Builder createPingRequest();

    RpcRequests.RequestVoteRequest.Builder createVoteRequest();

    RpcRequests.RequestVoteResponse.Builder createVoteResponse();

    RpcRequests.ErrorResponse.Builder createErrorResponse();

    LocalStorageOutter.StablePBMeta.Builder createStableMeta();

    RpcRequests.AppendEntriesRequest.Builder createAppendEntriesRequest();

    RpcRequests.AppendEntriesResponse.Builder createAppendEntriesResponse();

    RaftOutter.EntryMeta.Builder createEntryMeta();

    RpcRequests.TimeoutNowRequest.Builder createTimeoutNowRequest();

    RpcRequests.TimeoutNowResponse.Builder createTimeoutNowResponse();

    RpcRequests.ReadIndexRequest.Builder createReadIndexRequest();

    RpcRequests.ReadIndexResponse.Builder createReadIndexResponse();

    RaftOutter.SnapshotMeta.Builder createSnapshotMeta();

    LocalStorageOutter.LocalSnapshotPbMeta.Builder createLocalSnapshotMeta();

    LocalStorageOutter.LocalSnapshotPbMeta.File.Builder createFile();
}
