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

package org.apache.ignite.internal.cli.commands.cluster.config;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.FormatMixin;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

/**
 * Command that shows configuration from the cluster in REPL mode.
 */
@Command(name = "show", description = "Shows cluster configuration")
public class ClusterConfigShowReplCommand extends BaseCommand implements Runnable {
    /**
     * Cluster endpoint URL option.
     */
    @Mixin
    private ClusterUrlMixin clusterUrl;

    /**
     * Configuration selector option.
     */
    @Parameters(arity = "0..1", description = "Configuration path selector")
    private String selector;

    @Inject
    private ClusterConfigShowCall call;

    @Inject
    private ConnectToClusterQuestion question;

    @Mixin
    private FormatMixin format;

    @Override
    public void run() {
        question.askQuestionIfNotConnected(clusterUrl.getClusterUrl())
                .map(this::configShowCallInput)
                .then(Flows.fromCall(call))
                .exceptionHandler(ClusterNotInitializedExceptionHandler.createReplHandler("Cannot show cluster config"))
                .verbose(verbose)
                .print(format.decorator())
                .start();
    }

    private ClusterConfigShowCallInput configShowCallInput(String clusterUrl) {
        return ClusterConfigShowCallInput.builder().selector(selector).clusterUrl(clusterUrl).build();
    }
}
