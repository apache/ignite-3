/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cli.commands.cluster.init;

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_NAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_NAME_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CMG_NODE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CMG_NODE_NAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CMG_NODE_NAME_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.META_STORAGE_NODE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.META_STORAGE_NODE_NAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.META_STORAGE_NODE_NAME_OPTION_SHORT;

import java.util.ArrayList;
import java.util.List;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Mixin class for cluster init command options.
 */
public class ClusterInitOptions {
    /**
     * List of names of the nodes (each represented by a separate command line argument) that will host the Meta Storage. If the
     * "--cmg-nodes" parameter is omitted, the same nodes will also host the Cluster Management Group.
     */
    @Option(names = {META_STORAGE_NODE_NAME_OPTION, META_STORAGE_NODE_NAME_OPTION_SHORT},
            required = true,
            description = META_STORAGE_NODE_NAME_OPTION_DESC)
    private List<String> metaStorageNodes;

    /**
     * List of names of the nodes (each represented by a separate command line argument) that will host the Cluster Management Group.
     */
    @Option(names = {CMG_NODE_NAME_OPTION, CMG_NODE_NAME_OPTION_SHORT}, description = CMG_NODE_NAME_OPTION_DESC)
    private List<String> cmgNodes = new ArrayList<>();

    /** Name of the cluster. */
    @Option(names = {CLUSTER_NAME_OPTION, CLUSTER_NAME_OPTION_SHORT}, required = true, description = CLUSTER_NAME_OPTION_DESC)
    private String clusterName;

    @Mixin
    private AuthenticationOptions authenticationOptions;

    public List<String> metaStorageNodes() {
        return metaStorageNodes;
    }

    public List<String> cmgNodes() {
        return cmgNodes;
    }

    public String clusterName() {
        return clusterName;
    }

    public AuthenticationOptions authenticationOptions() {
        return authenticationOptions;
    }
}
