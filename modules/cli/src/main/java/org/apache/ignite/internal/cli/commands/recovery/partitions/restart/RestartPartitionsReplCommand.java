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

package org.apache.ignite.internal.cli.commands.recovery.partitions.restart;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.recovery.restart.RestartPartitionsCall;
import org.apache.ignite.internal.cli.call.recovery.restart.RestartPartitionsCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/** Command to restart partitions. */
@Command(name = "restart", description = "Restarts partitions.")
public class RestartPartitionsReplCommand extends BaseCommand implements Runnable {
    @Mixin
    private RestartPartitionsMixin options;

    @Inject
    private ConnectToClusterQuestion question;

    @Inject
    private RestartPartitionsCall call;

    @Override
    public void run() {
        question.askQuestionIfNotConnected(options.clusterUrl())
                .map(url -> RestartPartitionsCallInput.of(options, url))
                .then(Flows.fromCall(call))
                .verbose(verbose)
                .print()
                .start();
    }
}
