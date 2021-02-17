package org.apache.ignite.raft.client.service;

/**
 *
 */
public interface RaftGroupClientService {
    <T, R> R submit(T request);
}
