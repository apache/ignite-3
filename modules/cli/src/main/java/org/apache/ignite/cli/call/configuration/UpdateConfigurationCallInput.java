package org.apache.ignite.cli.call.configuration;

import org.apache.ignite.cli.core.call.CallInput;

/**
 * Input for {@link UpdateConfigurationCall}.
 */
public class UpdateConfigurationCallInput implements CallInput {
    /**
     * Node ID.
     */
    private final String nodeId;
    /**
     * Configuration to update.
     */
    private final String config;
    /**
     * Cluster url.
     */
    private final String clusterUrl;

    private UpdateConfigurationCallInput(String nodeId, String config, String clusterUrl) {
        this.nodeId = nodeId;
        this.config = config;
        this.clusterUrl = clusterUrl;
    }

    /**
     * Builder method.
     *
     * @return Builder for {@link UpdateConfigurationCallInput}.
     */
    public static UpdateConfigurationCallInputBuilder builder() {
        return new UpdateConfigurationCallInputBuilder();
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
     * Get configuration.
     *
     * @return Configuration to update.
     */
    public String getConfig() {
        return config;
    }

    /**
     * Get cluster URL.
     *
     * @return Cluster url.
     */
    public String getClusterUrl() {
        return clusterUrl;
    }

    /**
     * Builder for {@link UpdateConfigurationCallInput}.
     */
    public static class UpdateConfigurationCallInputBuilder {
        private String nodeId;
        private String config;
        private String clusterUrl;

        public UpdateConfigurationCallInputBuilder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public UpdateConfigurationCallInputBuilder config(String config) {
            this.config = config;
            return this;
        }

        public UpdateConfigurationCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public UpdateConfigurationCallInput build() {
            return new UpdateConfigurationCallInput(nodeId, config, clusterUrl);
        }
    }
}
