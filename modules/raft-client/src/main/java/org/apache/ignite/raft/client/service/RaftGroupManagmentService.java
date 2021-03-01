package org.apache.ignite.raft.client.service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.raft.PeerId;
import org.jetbrains.annotations.Nullable;

/** */
public interface RaftGroupManagmentService {
    /**
     * @param groupId
     * @return Leader id or null if it has not been yet initialized.
     */
    @Nullable PeerId getLeader(String groupId);

    /**
     * @param groupId
     * @return List of peers or null if it has not been yet initialized.
     */
    @Nullable List<PeerId> getPeers(String groupId);

    /**
     * @param groupId
     * @return List of peers or null if it has not been yet initialized.
     */
    @Nullable List<PeerId> getLearners(String groupId);

    /**
     * Adds a voting peer to the raft group.
     *
     * @param request   request data
     * @return A future with the result
     */
    CompletableFuture<PeersChangeState> addPeer(PeerId peerId);

    /**
     * Removes a peer from the raft group.
     *
     * @param endpoint  server address
     * @param request   request data
     * @return a future with result
     */
    CompletableFuture<PeersChangeState> removePeer(PeerId peerId);

    /**
     * Locally resets raft group peers. Intended for recovering from a group unavailability at the price of consistency.
     *
     * @param peerId Node to execute the configuration reset.
     * @param request   request data
     * @return A future with result.
     */
    CompletableFuture<Void> resetPeers(PeerId peerId, List<PeerId> peers);

    /**
     * Takes a local snapshot.
     *
     * @param peerId  Peer id.
     * @param request   request data
     * @param done      callback
     * @return a future with result.
     */
    CompletableFuture<Void> snapshot(PeerId peerId);

    /**
     * Change peers.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    CompletableFuture<PeersChangeState> changePeers(List<PeerId> peers);

    /**
     * Adds learners.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result.
     */
    CompletableFuture<PeersChangeState> addLearners(List<PeerId> peers);

    /**
     * Removes learners.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result
     */
    CompletableFuture<PeersChangeState> removeLearners(List<PeerId> peers);

    /**
     * Resets learners to new set.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result.
     */
    CompletableFuture<PeersChangeState> resetLearners(List<PeerId> peers);

    /**
     * Transfer leadership to other peer.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @return a future with result.
     */
    CompletableFuture<Void> transferLeader(PeerId newLeader);

    /**
     * Represents a change in peers list.
     */
    interface PeersChangeState {
        List<PeerId> getOld();

        List<PeerId> getNew();
    }
}
