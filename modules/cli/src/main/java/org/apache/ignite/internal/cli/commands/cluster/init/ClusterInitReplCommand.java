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

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_CONFIG_OPTION;
import static org.apache.ignite.internal.cli.commands.cluster.init.ClusterInitConstants.SPINNER_PREFIX;
import static org.apache.ignite.internal.cli.commands.cluster.init.ClusterInitConstants.SPINNER_UPDATE_INTERVAL_MILLIS;
import static org.apache.ignite.internal.cli.core.call.CallExecutionPipeline.asyncBuilder;
import static org.apache.ignite.internal.cli.core.style.component.QuestionUiComponent.fromYesNoQuestion;
import static picocli.CommandLine.Command;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.cluster.ClusterInitCallFactory;
import org.apache.ignite.internal.cli.call.cluster.ClusterInitCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.core.call.AsyncCall;
import org.apache.ignite.internal.cli.core.call.ProgressTracker;
import org.apache.ignite.internal.cli.core.flow.builder.FlowBuilder;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import org.apache.ignite.internal.cli.core.repl.ConnectionHeartBeat;
import org.apache.ignite.internal.cli.core.style.component.QuestionUiComponent;
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
    private ClusterInitCallFactory callFactory;

    @Inject
    private ConnectionHeartBeat connectionHeartBeat;

    @Override
    public void run() {
        runFlow(askQuestionIfConfigIsPath()
                .then(Flows.mono(this::runAsync))
        );
    }

    private FlowBuilder<Void, ClusterInitCallInput> askQuestionIfConfigIsPath() {
        try {
            return Flows.from(buildCallInput());
        } catch (ConfigAsPathException e) {
            QuestionUiComponent questionUiComponent = fromYesNoQuestion(
                    "It seems that you have passed the path to the configuration file in the configuration content "
                            + CLUSTER_CONFIG_OPTION + " option. "
                            + "Do you want to read cluster configuration from this file?"
            );

            return Flows.acceptQuestion(questionUiComponent, this::buildCallInputFromConfigFile);
        }
    }

    private ClusterInitCallInput buildCallInput() {
        return ClusterInitCallInput.builder()
                .clusterUrl(clusterUrl.getClusterUrl())
                .fromClusterInitOptions(clusterInitOptions)
                .build();
    }

    private ClusterInitCallInput buildCallInputFromConfigFile() {
        return ClusterInitCallInput.builder()
                .clusterUrl(clusterUrl.getClusterUrl())
                .clusterConfiguration(clusterInitOptions.readConfigAsPath())
                .cmgNodes(clusterInitOptions.cmgNodes())
                .metaStorageNodes(clusterInitOptions.metaStorageNodes())
                .clusterName(clusterInitOptions.clusterName())
                .build();
    }

    private int runAsync(ClusterInitCallInput input) {
        return runPipeline(
                asyncBuilder(this::createCall)
                        .inputProvider(() -> input)
                        .enableSpinner(SPINNER_PREFIX)
                        .updateIntervalMillis(SPINNER_UPDATE_INTERVAL_MILLIS)
        );
    }

    private AsyncCall<ClusterInitCallInput, String> createCall(ProgressTracker tracker) {
        AsyncCall<ClusterInitCallInput, String> delegate = callFactory.create(tracker);
        return input -> delegate.execute(input)
                // Refresh connected state immediately after execution because node state is unavailable during cluster initialization
                .whenComplete((output, throwable) -> connectionHeartBeat.pingConnection());
    }
}
