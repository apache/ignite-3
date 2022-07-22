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

package org.apache.ignite.cli.commands.cluster.status;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.cluster.status.ClusterStatusCall;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.decorators.ClusterStatusDecorator;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.StatusCallInput;
import org.apache.ignite.cli.core.repl.Session;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command that prints status of ignite cluster.
 */
@Command(name = "status", description = "Prints status of the cluster.")
@Singleton
public class ClusterStatusReplSubCommand extends BaseCommand implements Runnable {

    /**
     * Cluster url option.
     */
    @SuppressWarnings("PMD.UnusedPrivateField")
    @Option(names = {"--cluster-url"}, description = "Url to cluster node.", descriptionKey = "ignite.cluster-url")
    private String clusterUrl;

    @Inject
    private ClusterStatusCall clusterStatusReplCall;

    @Inject
    private Session session;

    /** {@inheritDoc} */
    @Override
    public void run() {
        String inputUrl;

        if (clusterUrl != null) {
            inputUrl = clusterUrl;
        } else if (session.isConnectedToNode()) {
            inputUrl = session.nodeUrl();
        } else {
            spec.commandLine().getErr().println("You are not connected to node. Run 'connect' command or use '--cluster-url' option.");
            return;
        }

        CallExecutionPipeline.builder(clusterStatusReplCall)
                .inputProvider(() -> new StatusCallInput(inputUrl))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new ClusterStatusDecorator())
                .build()
                .runPipeline();
    }
}
