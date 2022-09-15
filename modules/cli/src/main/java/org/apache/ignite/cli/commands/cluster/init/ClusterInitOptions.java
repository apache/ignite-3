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

import java.util.ArrayList;
import java.util.List;
import picocli.CommandLine.Option;

/**
 * Mixin class for cluster init command options.
 */
public class ClusterInitOptions {
    /**
     * List of names of the nodes (each represented by a separate command line argument) that will host the Meta Storage. If the
     * "--cmg-nodes" parameter is omitted, the same nodes will also host the Cluster Management Group.
     */
    @Option(names = {"-m", "--meta-storage-node"}, required = true, description = {
            "Name of the node (repeat like '--meta-storage-node node1 --meta-storage-node node2' to specify more than one node)",
            "that will host the Meta Storage.",
            "If the --cmg-node parameter is omitted, the same nodes will also host the Cluster Management Group."
    })
    private List<String> metaStorageNodes;

    /**
     * List of names of the nodes (each represented by a separate command line argument) that will host the Cluster Management Group.
     */
    @Option(names = {"-c", "--cmg-node"}, description = {
            "Name of the node (repeat like '--cmg-node node1 --cmg-node node2' to specify more than one node)",
            "that will host the Cluster Management Group.",
            "If omitted, then --meta-storage-node values will also supply the nodes for the Cluster Management Group."
    })
    private List<String> cmgNodes = new ArrayList<>();

    /** Name of the cluster. */
    @Option(names = {"-n", "--cluster-name"}, required = true, description = "Human-readable name of the cluster")
    private String clusterName;

    public List<String> getMetaStorageNodes() {
        return metaStorageNodes;
    }

    public List<String> getCmgNodes() {
        return cmgNodes;
    }

    public String getClusterName() {
        return clusterName;
    }
}
