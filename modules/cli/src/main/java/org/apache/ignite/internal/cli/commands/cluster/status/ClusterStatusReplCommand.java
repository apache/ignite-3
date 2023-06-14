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

package org.apache.ignite.internal.cli.commands.cluster.status;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterStatusCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/**
 * Command that prints status of ignite cluster.
 */
@Command(name = "status", description = "Prints status of the cluster")
public class ClusterStatusReplCommand extends BaseCommand implements Runnable {
    /** Cluster endpoint URL option. */
    @Mixin
    private ClusterUrlMixin clusterUrl;

    @Inject
    private ClusterStatusCall call;

    @Inject
    private ConnectToClusterQuestion question;

    /** {@inheritDoc} */
    @Override
    public void run() {
        question.askQuestionIfNotConnected(clusterUrl.getClusterUrl())
                .map(UrlCallInput::new)
                .then(Flows.fromCall(call))
                .verbose(verbose)
                .print()
                .start();
    }
}
