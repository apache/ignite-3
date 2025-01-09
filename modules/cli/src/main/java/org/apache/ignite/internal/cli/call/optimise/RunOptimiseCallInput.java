package org.apache.ignite.internal.cli.call.optimise;

import org.apache.ignite.internal.cli.core.call.CallInput;

public class RunOptimiseCallInput implements CallInput {
    private final String clusterUrl;
    private final String nodeName;
    private final boolean writeIntensive;

    public RunOptimiseCallInput(String clusterUrl, String nodeName, boolean writeIntensive) {
        this.clusterUrl = clusterUrl;
        this.nodeName = nodeName;
        this.writeIntensive = writeIntensive;
    }

    public static RunOptimiseCallInputBuilder builder() {
        return new RunOptimiseCallInputBuilder();
    }

    public String getClusterUrl() {
        return clusterUrl;
    }

    public boolean getWriteIntensive() {
        return writeIntensive;
    }

    public String getNodeName() {
        return nodeName;
    }

    public static class RunOptimiseCallInputBuilder {
        private String clusterUrl;
        private String nodeName;
        private boolean writeIntensive;

        public RunOptimiseCallInputBuilder setClusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;

            return this;
        }

        public RunOptimiseCallInputBuilder setNodeName(String nodeName) {
            this.nodeName = nodeName;

            return this;
        }

        public RunOptimiseCallInputBuilder setWriteIntensive(boolean writeIntensive) {
            this.writeIntensive = writeIntensive;

            return this;
        }

        public RunOptimiseCallInput build() {
            return new RunOptimiseCallInput(clusterUrl, nodeName, writeIntensive);
        }
    }
}
