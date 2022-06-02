package org.apache.ignite.cli.call.status;

/**
 * Class that represents the cluster status.
 */
public class Status {
    private final int nodeCount;
    private final boolean initialized;
    private final String name;
    private final boolean connected;
    private final String connectedNodeUrl;

    private Status(int nodeCount, boolean initialized, String name, boolean connected, String connectedNodeUrl) {
        this.nodeCount = nodeCount;
        this.initialized = initialized;
        this.name = name;
        this.connected = connected;
        this.connectedNodeUrl = connectedNodeUrl;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public String getName() {
        return name;
    }

    public boolean isConnected() {
        return connected;
    }

    public String getConnectedNodeUrl() {
        return connectedNodeUrl;
    }

    /**
     * Builder for {@link Status}.
     */
    public static StatusBuilder builder() {
        return new StatusBuilder();
    }

    /**
     * Builder for {@link Status}.
     */
    public static class StatusBuilder {
        private int nodeCount;
        private boolean initialized;
        private String name;
        private boolean connected;
        private String connectedNodeUrl;

        private StatusBuilder() {

        }

        public StatusBuilder nodeCount(int nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public StatusBuilder initialized(boolean initialized) {
            this.initialized = initialized;
            return this;
        }

        public StatusBuilder name(String name) {
            this.name = name;
            return this;
        }

        public StatusBuilder connected(boolean connected) {
            this.connected = connected;
            return this;
        }

        public StatusBuilder connectedNodeUrl(String connectedNodeUrl) {
            this.connectedNodeUrl = connectedNodeUrl;
            return this;
        }

        public Status build() {
            return new Status(nodeCount, initialized, name, connected, connectedNodeUrl);
        }
    }
}
