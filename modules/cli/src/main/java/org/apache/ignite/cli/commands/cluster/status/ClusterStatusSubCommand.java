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

import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_DESC;
import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_KEY;
import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_OPTION;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.call.cluster.status.ClusterStatusCall;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.decorators.ClusterStatusDecorator;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.StatusCallInput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command that prints status of ignite cluster.
 */
@Command(name = "status",
        aliases = "cluster show", //TODO: https://issues.apache.org/jira/browse/IGNITE-17102
        description = "Prints status of the cluster.")
@Singleton
public class ClusterStatusSubCommand extends BaseCommand implements Callable<Integer> {

    /**
     * Cluster endpoint URL option.
     */
    @SuppressWarnings("PMD.UnusedPrivateField")
    @Option(names = {CLUSTER_URL_OPTION}, description = CLUSTER_URL_DESC, descriptionKey = CLUSTER_URL_KEY)
    private String clusterUrl;

    @Inject
    private ClusterStatusCall clusterStatusCall;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(clusterStatusCall)
                .inputProvider(() -> new StatusCallInput(clusterUrl))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new ClusterStatusDecorator())
                .build()
                .runPipeline();
    }
}
