/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.deprecated.spec;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.deprecated.builtins.cluster.ClusterApiClient;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Commands for managing Ignite cluster as a whole.
 */
@CommandLine.Command(
        name = "cluster",
        description = "Manages an Ignite cluster as a whole.",
        subcommands = {
                ClusterCommandSpec.InitClusterCommandSpec.class,
        }
)
public class ClusterCommandSpec {
    /**
     * Initializes an Ignite cluster.
     */
    @CommandLine.Command(name = "init", description = "Initializes an Ignite cluster.")
    public static class InitClusterCommandSpec extends BaseCommand {

        @Inject
        private ClusterApiClient clusterApiClient;

        /**
         * Address of the REST endpoint of the receiving node in host:port format.
         */
        @Option(names = "--node-endpoint", required = true,
                description = "Address of the REST endpoint of the receiving node in host:port format")
        private String nodeEndpoint;

        /**
         * List of names of the nodes (each represented by a separate command line argument) that will host the Metastorage Raft group.
         * If the "--cmg-nodes" parameter is omitted, the same nodes will also host the Cluster Management Raft group.
         */
        @Option(names = "--meta-storage-node", required = true, description = {
                "Name of the node (repeat like '--meta-storage-node node1 --meta-storage-node node2' to specify more than one node)",
                "that will host the Meta Storage Raft group.",
                "If the --cmg-node parameter is omitted, the same nodes will also host the Cluster Management Raft group."
        })
        private List<String> metaStorageNodes;

        /**
         * List of names of the nodes (each represented by a separate command line argument) that will host
         * the Cluster Management Raft group.
         */
        @Option(names = "--cmg-node", description = {
                "Name of the node (repeat like '--cmg-node node1 --cmg-node node2' to specify more than one node)",
                "that will host the Cluster Management Raft group.",
                "If omitted, then --meta-storage-node values will also supply the nodes for the Cluster Management Raft group."
        })
        private List<String> cmgNodes = new ArrayList<>();

        /** Name of the cluster. */
        @Option(names = "--cluster-name", required = true, description = "Human-readable name of the cluster")
        private String clusterName;

        /** {@inheritDoc} */
        @Override
        public void run() {
            clusterApiClient.init(
                    nodeEndpoint,
                    metaStorageNodes,
                    cmgNodes,
                    clusterName,
                    spec.commandLine().getOut()
            );
        }
    }
}
