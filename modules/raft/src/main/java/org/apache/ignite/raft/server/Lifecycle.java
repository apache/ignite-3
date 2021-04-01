package org.apache.ignite.raft.server;

public interface Lifecycle<T> {
    void init(T option) throws Exception;

    void destroy() throws Exception;
}
