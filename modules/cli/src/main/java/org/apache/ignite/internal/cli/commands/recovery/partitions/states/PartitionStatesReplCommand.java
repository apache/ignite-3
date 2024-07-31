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

package org.apache.ignite.internal.cli.commands.recovery.partitions.states;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.recovery.states.PartitionStatesCall;
import org.apache.ignite.internal.cli.call.recovery.states.PartitionStatesCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import org.apache.ignite.internal.cli.decorators.TableDecorator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/** Command to get partition states. */
@Command(name = "states", description = "Returns partition states.")
public class PartitionStatesReplCommand extends BaseCommand implements Runnable {
    /** Cluster endpoint URL option. */
    @Mixin
    private ClusterUrlMixin clusterUrl;

    @Mixin
    private PartitionStatesMixin options;

    @Inject
    private ConnectToClusterQuestion question;

    @Inject
    private PartitionStatesCall call;

    @Override
    public void run() {
        question.askQuestionIfNotConnected(clusterUrl.getClusterUrl())
                .map(url -> PartitionStatesCallInput.of(options, url))
                .then(Flows.fromCall(call))
                .print(new TableDecorator(options.plain()))
                .exceptionHandler(ClusterNotInitializedExceptionHandler.createReplHandler("Cannot list partition states"))
                .verbose(verbose)
                .start();
    }

}
