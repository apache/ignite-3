package org.apache.ignite.raft.client.service;

import java.util.concurrent.CompletableFuture;

/**
 *
 */
public interface RaftGroupClientRequestService {
    /**
     * Submits a custom request to a raft group leader. If a leader is not known yet, will try to resolve the leader.
     * @param request
     * @param <T> Request.
     * @param <R> Response.
     * @return A future.
     */
    <T, R> CompletableFuture<R> submit(T request);
}
