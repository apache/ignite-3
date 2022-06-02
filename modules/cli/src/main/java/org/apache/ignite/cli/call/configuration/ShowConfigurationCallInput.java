package org.apache.ignite.cli.call.configuration;

import org.apache.ignite.cli.core.call.CallInput;

/**
 * Input for {@link ShowConfigurationCall}.
 */
public class ShowConfigurationCallInput implements CallInput {
    /**
     * Node ID.
     */
    private final String nodeId;
    /**
     * Selector for configuration tree.
     */
    private final String selector;
    /**
     * Cluster url.
     */
    private final String clusterUrl;

    private ShowConfigurationCallInput(String nodeId, String selector, String clusterUrl) {
        this.nodeId = nodeId;
        this.selector = selector;
        this.clusterUrl = clusterUrl;
    }

    /**
     * Get node ID.
     *
     * @return Node ID.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Get selector.
     *
     * @return Selector for configuration tree.
     */
    public String getSelector() {
        return selector;
    }

    /**
     * Get cluster URL.
     *
     * @return Cluster URL.
     */
    public String getClusterUrl() {
        return clusterUrl;
    }

    /**
     * Builder for {@link ShowConfigurationCallInput}.
     */
    public static ShowConfigurationCallInputBuilder builder() {
        return new ShowConfigurationCallInputBuilder();
    }

    /**
     * Builder for {@link ShowConfigurationCallInput}.
     */
    public static class ShowConfigurationCallInputBuilder {
        private String nodeId;
        private String selector;
        private String clusterUrl;

        public ShowConfigurationCallInputBuilder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public ShowConfigurationCallInputBuilder selector(String selector) {
            this.selector = selector;
            return this;
        }

        public ShowConfigurationCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public ShowConfigurationCallInput build() {
            return new ShowConfigurationCallInput(nodeId, selector, clusterUrl);
        }
    }
}
