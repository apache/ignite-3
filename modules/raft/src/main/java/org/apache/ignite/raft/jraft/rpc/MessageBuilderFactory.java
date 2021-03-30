package org.apache.ignite.raft.jraft.rpc;

import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter;
import org.apache.ignite.raft.jraft.entity.LocalStorageOutter;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.rpc.message.DefaultMessageBuilderFactory;

// TODO asch use JRaftServiceLoader ?
public interface MessageBuilderFactory {
    public static MessageBuilderFactory DEFAULT = new DefaultMessageBuilderFactory();

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

    RpcRequests.InstallSnapshotRequest.Builder createInstallSnapshotRequest();

    RpcRequests.InstallSnapshotResponse.Builder createInstallSnapshotResponse();

    RpcRequests.GetFileRequest.Builder createGetFileRequest();

    RpcRequests.GetFileResponse.Builder createGetFileResponse();

    // CLI
    CliRequests.AddPeerRequest.Builder createAddPeerRequest();

    CliRequests.AddPeerResponse.Builder createAddPeerResponse();

    CliRequests.RemovePeerRequest.Builder createRemovePeerRequest();

    CliRequests.RemovePeerResponse.Builder createRemovePeerResponse();

    CliRequests.ChangePeersRequest.Builder createChangePeerRequest();

    CliRequests.ChangePeersResponse.Builder createChangePeerResponse();

    CliRequests.SnapshotRequest.Builder createSnapshotRequest();

    CliRequests.ResetPeerRequest.Builder createResetPeerRequest();

    CliRequests.TransferLeaderRequest.Builder createTransferLeaderRequest();

    CliRequests.GetLeaderRequest.Builder createGetLeaderRequest();

    CliRequests.GetLeaderResponse.Builder createGetLeaderResponse();

    CliRequests.GetPeersRequest.Builder createGetPeersRequest();

    CliRequests.GetPeersResponse.Builder createGetPeersResponse();

    CliRequests.AddLearnersRequest.Builder createAddLearnersRequest();

    CliRequests.RemoveLearnersRequest.Builder createRemoveLearnersRequest();

    CliRequests.ResetLearnersRequest.Builder createResetLearnersRequest();

    CliRequests.LearnersOpResponse.Builder createLearnersOpResponse();
}
