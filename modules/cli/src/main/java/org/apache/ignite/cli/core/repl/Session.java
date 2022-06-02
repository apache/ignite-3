package org.apache.ignite.cli.core.repl;

import jakarta.inject.Singleton;

/**
 * Connection session that in fact is holder for state: connected or disconnected.
 * Also has a nodeUrl if the state is connected.
 */
@Singleton
public class Session {
    private boolean connectedToNode;
    private String nodeUrl;
    private String jdbcUrl;

    public boolean isConnectedToNode() {
        return connectedToNode;
    }

    public void setConnectedToNode(boolean connectedToNode) {
        this.connectedToNode = connectedToNode;
    }

    public String getNodeUrl() {
        return nodeUrl;
    }

    public void setNodeUrl(String nodeUrl) {
        this.nodeUrl = nodeUrl;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }
}
