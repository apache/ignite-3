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

package org.apache.ignite.cli.commands.cluster;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.call.cluster.ClusterInitCall;
import org.apache.ignite.cli.call.cluster.ClusterInitCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Initializes an Ignite cluster.
 */
@Command(name = "init", description = "Initializes an Ignite cluster.")
public class ClusterInitSubCommand extends BaseCommand implements Callable<Integer> {

    /**
     * Node url option.
     */
    @Option(
            names = {"--cluster-url"}, description = "Url to ignite node.",
            descriptionKey = "ignite.cluster-url", defaultValue = "http://localhost:10300"
    )
    private String clusterUrl;

    /**
     * List of names of the nodes (each represented by a separate command line argument) that will host the Metastorage Raft group. If the
     * "--cmg-nodes" parameter is omitted, the same nodes will also host the Cluster Management Raft group.
     */
    @Option(names = "--meta-storage-node", required = true, description = {
            "Name of the node (repeat like '--meta-storage-node node1 --meta-storage-node node2' to specify more than one node)",
            "that will host the Meta Storage Raft group.",
            "If the --cmg-node parameter is omitted, the same nodes will also host the Cluster Management Raft group."
    })
    private List<String> metaStorageNodes;

    /**
     * List of names of the nodes (each represented by a separate command line argument) that will host the Cluster Management Raft group.
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

    @Inject
    private ClusterInitCall call;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }

    private ClusterInitCallInput buildCallInput() {
        return ClusterInitCallInput.builder()
                .clusterUrl(clusterUrl)
                .metaStorageNodes(metaStorageNodes)
                .cmgNodes(cmgNodes)
                .clusterName(clusterName)
                .build();
    }
}
