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

package org.apache.ignite.cli.commands.cluster.init;

import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_DESC;
import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_KEY;
import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_OPTION;
import static picocli.CommandLine.Command;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cli.call.cluster.ClusterInitCall;
import org.apache.ignite.cli.call.cluster.ClusterInitCallInput;
import org.apache.ignite.cli.call.cluster.ClusterInitCallInput.ClusterInitCallInputBuilder;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.repl.Session;
import picocli.CommandLine.Option;

/**
 * Initializes an Ignite cluster.
 */
@Command(name = "init", description = "Initializes an Ignite cluster.")
public class ClusterInitReplSubCommand extends BaseCommand implements Runnable {

    /**
     * Cluster endpoint URL option.
     */
    @Option(
            names = {CLUSTER_URL_OPTION}, description = CLUSTER_URL_DESC, descriptionKey = CLUSTER_URL_KEY,
            defaultValue = "http://localhost:10300"
    )
    private String clusterUrl;

    /**
     * List of names of the nodes (each represented by a separate command line argument) that will host the Meta Storage. If the
     * "--cmg-nodes" parameter is omitted, the same nodes will also host the Cluster Management Group.
     */
    @Option(names = "--meta-storage-node", required = true, description = {
            "Name of the node (repeat like '--meta-storage-node node1 --meta-storage-node node2' to specify more than one node)",
            "that will host the Meta Storage.",
            "If the --cmg-node parameter is omitted, the same nodes will also host the Cluster Management Group."
    })
    private List<String> metaStorageNodes;

    /**
     * List of names of the nodes (each represented by a separate command line argument) that will host the Cluster Management Group.
     */
    @Option(names = "--cmg-node", description = {
            "Name of the node (repeat like '--cmg-node node1 --cmg-node node2' to specify more than one node)",
            "that will host the Cluster Management Group.",
            "If omitted, then --meta-storage-node values will also supply the nodes for the Cluster Management Group."
    })
    private List<String> cmgNodes = new ArrayList<>();

    /** Name of the cluster. */
    @Option(names = "--cluster-name", required = true, description = "Human-readable name of the cluster")
    private String clusterName;

    @Inject
    private ClusterInitCall call;

    @Inject
    private Session session;

    /** {@inheritDoc} */
    @Override
    public void run() {
        ClusterInitCallInputBuilder input = buildCallInput();

        if (session.isConnectedToNode()) {
            input.clusterUrl(session.nodeUrl());
        } else if (clusterUrl != null) {
            input.clusterUrl(clusterUrl);
        } else {
            spec.commandLine().getErr().println("You are not connected to node. Run 'connect' command or use '"
                    + CLUSTER_URL_OPTION + "' option.");
            return;
        }

        CallExecutionPipeline.builder(call)
                .inputProvider(input::build)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }

    private ClusterInitCallInputBuilder buildCallInput() {
        return ClusterInitCallInput.builder()
                .metaStorageNodes(metaStorageNodes)
                .cmgNodes(cmgNodes)
                .clusterName(clusterName);
    }
}
