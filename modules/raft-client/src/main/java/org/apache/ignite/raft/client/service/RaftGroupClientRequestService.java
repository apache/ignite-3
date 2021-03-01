package org.apache.ignite.raft.client.service;

import java.util.concurrent.CompletableFuture;

/**
 *
 */
public interface RaftGroupClientRequestService {
    /**
     * Submits a custom request to a raft group leader. If a leader is not initialized yet, will try to resolve it.
     * @param request
     * @param <T> Request.
     * @param <R> Response.
     * @return A future.
     */
    <R> CompletableFuture<R> submit(Object request);
}
