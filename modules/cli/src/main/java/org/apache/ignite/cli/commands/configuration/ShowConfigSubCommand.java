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

package org.apache.ignite.cli.commands.configuration;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.configuration.ShowConfigurationCall;
import org.apache.ignite.cli.call.configuration.ShowConfigurationCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.decorators.JsonDecorator;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command that shows configuration from the cluster.
 */
@Command(name = "show",
        description = "Shows configuration.")
public class ShowConfigSubCommand extends BaseCommand {

    /**
     * Node ID option.
     */
    @Option(names = {"--node"}, description = "Node ID to get local configuration.")
    private String nodeId;
    /**
     * Configuration selector option.
     */
    @Option(names = {"--selector"}, description = "Configuration path selector.")
    private String selector;

    /**
     * Cluster url option.
     */
    @Option(
            names = {"--cluster-url"}, description = "Url to cluster node.",
            descriptionKey = "ignite.cluster-url", defaultValue = "http://localhost:10300"
    )
    private String clusterUrl;

    @Inject
    private ShowConfigurationCall call;

    /** {@inheritDoc} */
    @Override
    public void run() {
        CallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new JsonDecorator())
                .build()
                .runPipeline();
    }

    private ShowConfigurationCallInput buildCallInput() {
        return ShowConfigurationCallInput.builder()
                .clusterUrl(clusterUrl)
                .selector(selector)
                .nodeId(nodeId)
                .build();
    }
}
