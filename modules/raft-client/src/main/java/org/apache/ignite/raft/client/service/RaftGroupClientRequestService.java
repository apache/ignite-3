package org.apache.ignite.raft.client.service;

import java.util.concurrent.CompletableFuture;

/**
 *
 */
public interface RaftGroupClientRequestService {
    <T, R> CompletableFuture<R> submit(T request);
}
