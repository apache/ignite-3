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

package org.apache.ignite.cli.spec;

import static java.util.Collections.emptyList;

import java.util.List;
import javax.inject.Inject;
import org.apache.ignite.cli.builtins.cluster.ClusterApiClient;
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
public class ClusterCommandSpec extends CategorySpec {
    /**
     * Initializes an Ignite cluster.
     */
    @CommandLine.Command(name = "init", description = "Initializes an Ignite cluster.")
    public static class InitClusterCommandSpec extends CommandSpec {

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
        @Option(names = "--metastorage-nodes", required = true, arity = "1..*", description = {
                "List of names of the nodes (each represented by a separate command line argument)",
                "that will host the Metastorage Raft group.",
                "If the \"--cmg-nodes\" parameter is empty, the same nodes will also host the Cluster Management Raft group."
        })
        private List<String> metastorageNodes;

        /**
         * List of names of the nodes (each represented by a separate command line argument) that will host
         * the Cluster Management Raft group.
         */
        @Option(names = "--cmg-nodes", arity = "0..*", description = {
                "List of names of the nodes (each represented by a separate command line argument)",
                "that will host the Cluster Management Raft group.",
                "If omitted, then --metastorage-nodes will also supply the nodes for the Cluster Management Raft group."
        })
        private List<String> cmgNodes;

        /** {@inheritDoc} */
        @Override
        public void run() {
            clusterApiClient.init(
                    nodeEndpoint,
                    metastorageNodes,
                    cmgNodes == null ? emptyList() : cmgNodes,
                    spec.commandLine().getOut()
            );
        }
    }
}
