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

package org.apache.ignite.cli.commands.cluster.config;

import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_DESC;
import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_KEY;
import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_OPTION;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.cli.core.exception.handler.ShowConfigExceptionHandler;
import org.apache.ignite.cli.core.flow.Flowable;
import org.apache.ignite.cli.core.flow.builder.Flows;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command that shows configuration from the cluster in REPL mode.
 */
@Command(name = "show", description = "Shows cluster configuration")
public class ClusterConfigShowReplCommand extends BaseCommand implements Runnable {
    /**
     * Cluster endpoint URL option.
     */
    @Option(names = {CLUSTER_URL_OPTION}, description = CLUSTER_URL_DESC, descriptionKey = CLUSTER_URL_KEY)
    private String clusterUrl;

    /**
     * Configuration selector option.
     */
    @Parameters(arity = "0..1", description = "Configuration path selector")
    private String selector;

    @Inject
    private ClusterConfigShowCall call;

    @Inject
    private ConnectToClusterQuestion question;

    @Override
    public void run() {
        question.askQuestionIfNotConnected(clusterUrl)
                .exceptionHandler(new ShowConfigExceptionHandler())
                .map(this::configShowCallInput)
                .then(Flows.fromCall(call))
                .toOutput(spec.commandLine().getOut(), spec.commandLine().getErr())
                .build()
                .start(Flowable.empty());
    }

    private ClusterConfigShowCallInput configShowCallInput(String clusterUrl) {
        return ClusterConfigShowCallInput.builder().selector(selector).clusterUrl(clusterUrl).build();
    }
}
