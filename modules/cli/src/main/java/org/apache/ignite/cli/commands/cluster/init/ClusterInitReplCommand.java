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

import static org.apache.ignite.cli.core.style.component.CommonMessages.CONNECT_OR_USE_CLUSTER_URL_MESSAGE;
import static picocli.CommandLine.Command;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.cluster.ClusterInitCall;
import org.apache.ignite.cli.call.cluster.ClusterInitCallInput;
import org.apache.ignite.cli.call.cluster.ClusterInitCallInput.ClusterInitCallInputBuilder;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.repl.Session;
import picocli.CommandLine.Mixin;

/**
 * Initializes an Ignite cluster.
 */
@Command(name = "init", description = "Initializes an Ignite cluster")
public class ClusterInitReplCommand extends BaseCommand implements Runnable {
    /** Cluster endpoint URL option. */
    @Mixin
    private ClusterUrlMixin clusterUrl;

    @Mixin
    private ClusterInitOptions clusterInitOptions;

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
        } else if (clusterUrl.getClusterUrl() != null) {
            input.clusterUrl(clusterUrl.getClusterUrl());
        } else {
            spec.commandLine().getErr().println(CONNECT_OR_USE_CLUSTER_URL_MESSAGE.render());
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
        return ClusterInitCallInput.builder().fromClusterInitOptions(clusterInitOptions);
    }
}
