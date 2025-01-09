package org.phillippko.ignite.internal.cli.optimise.call;

import org.apache.ignite.internal.cli.core.call.CallInput;

public class RunBenchmarkCallInput implements CallInput {
    private final String clusterUrl;

    private final String benchmarkFilePath;
    private final String nodeName;

    public RunBenchmarkCallInput(String clusterUrl, String nodeName, String benchmarkFilePath) {
        this.clusterUrl = clusterUrl;
        this.benchmarkFilePath = benchmarkFilePath;
        this.nodeName = nodeName;
    }

    public static RunBenchmarkCallInputBuilder builder() {
        return new RunBenchmarkCallInputBuilder();
    }

    public String getClusterUrl() {
        return clusterUrl;
    }

    public String getBenchmarkFilePath() {
        return benchmarkFilePath;
    }

    public String getNodeName() {
        return nodeName;
    }

    public static class RunBenchmarkCallInputBuilder {
        private String clusterUrl;
        private String benchmarkFilePath;
        private String nodeName;

        public RunBenchmarkCallInputBuilder setClusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;

            return this;
        }

        public RunBenchmarkCallInputBuilder setBenchmarkFilePath(String benchmarkFilePath) {
            this.benchmarkFilePath = benchmarkFilePath;

            return this;
        }

        public RunBenchmarkCallInputBuilder setNodeName(String nodeName) {
            this.nodeName = nodeName;

            return this;
        }

        public RunBenchmarkCallInput build() {
            return new RunBenchmarkCallInput(clusterUrl, nodeName, benchmarkFilePath);
        }
    }
}
