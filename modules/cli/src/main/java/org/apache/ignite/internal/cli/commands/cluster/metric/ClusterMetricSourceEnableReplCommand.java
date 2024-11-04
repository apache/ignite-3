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

package org.apache.ignite.internal.cli.commands.cluster.metric;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.cluster.metric.ClusterMetricSourceEnableCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.commands.metric.MetricSourceMixin;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/** Command that enables cluster metric source in REPL mode. */
@Command(name = "enable", description = "Enables cluster metric source")
public class ClusterMetricSourceEnableReplCommand extends BaseCommand implements Runnable {
    @Mixin
    private ClusterUrlMixin clusterUrl;

    @Mixin
    private MetricSourceMixin metricSource;

    @Inject
    private ClusterMetricSourceEnableCall call;

    @Inject
    private ConnectToClusterQuestion question;

    @Override
    public void run() {
        runFlow(question.askQuestionIfNotConnected(clusterUrl.getClusterUrl())
                .map(metricSource::buildEnableCallInput)
                .then(Flows.fromCall(call))
                .exceptionHandler(ClusterNotInitializedExceptionHandler.createReplHandler("Cannot enable metrics"))
                .print()
        );
    }
}
