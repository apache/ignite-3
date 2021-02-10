package org.apache.ignite.raft.rpc;

/**
 * Basic RPC options.
 */
public class RpcOptions {
    private int rpcConnectTimeoutMs = 5_000;
    private int rpcDefaultTimeout = 5_000;

    public int getRpcConnectTimeoutMs() {
        return rpcConnectTimeoutMs;
    }

    public void setRpcConnectTimeoutMs(int rpcConnectTimeoutMs) {
        this.rpcConnectTimeoutMs = rpcConnectTimeoutMs;
    }

    public int getRpcDefaultTimeout() {
        return rpcDefaultTimeout;
    }

    public void setRpcDefaultTimeout(int rpcDefaultTimeout) {
        this.rpcDefaultTimeout = rpcDefaultTimeout;
    }
}
