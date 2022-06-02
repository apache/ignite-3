package org.apache.ignite.cli.call.connect;

import org.apache.ignite.cli.core.call.CallInput;

/**
 * Input for connect call to Ignite 3 node.
 */
public class ConnectCallInput implements CallInput {
    private final String nodeUrl;

    ConnectCallInput(String nodeUrl) {
        this.nodeUrl = nodeUrl;
    }

    public String getNodeUrl() {
        return nodeUrl;
    }

    public static ConnectCallInputBuilder builder() {
        return new ConnectCallInputBuilder();
    }

    /**
     * Builder for {@link ConnectCall}.
     */
    public static class ConnectCallInputBuilder {
        private String nodeUrl;

        public ConnectCallInputBuilder nodeUrl(String nodeUrl) {
            this.nodeUrl = nodeUrl;
            return this;
        }

        public ConnectCallInput build() {
            return new ConnectCallInput(nodeUrl);
        }
    }
}
