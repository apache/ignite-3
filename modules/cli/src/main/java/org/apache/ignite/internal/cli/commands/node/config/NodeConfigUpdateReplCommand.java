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

package org.apache.ignite.internal.cli.commands.node.config;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.configuration.NodeConfigUpdateCall;
import org.apache.ignite.internal.cli.call.configuration.NodeConfigUpdateCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.SpacedParameterMixin;
import org.apache.ignite.internal.cli.commands.node.NodeUrlMixin;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/**
 * Command that updates configuration in REPL mode.
 */
@Command(name = "update", description = "Updates node configuration")
public class NodeConfigUpdateReplCommand extends BaseCommand implements Runnable {
    /** Node URL option. */
    @Mixin
    private NodeUrlMixin nodeUrl;

    /** Configuration that will be updated. */
    @Mixin
    private SpacedParameterMixin configFromArgsAndFile;

    @Inject
    NodeConfigUpdateCall call;

    @Inject
    private ConnectToClusterQuestion question;

    /** {@inheritDoc} */
    @Override
    public void run() {
        runFlow(question.askQuestionIfNotConnected(nodeUrl.getNodeUrl())
                .map(this::nodeConfigUpdateCallInput)
                .then(Flows.fromCall(call))
                .print()
        );
    }

    private NodeConfigUpdateCallInput nodeConfigUpdateCallInput(String nodeUrl) {
        return NodeConfigUpdateCallInput.builder()
                .config(configFromArgsAndFile.formUpdateConfig())
                .nodeUrl(nodeUrl)
                .build();
    }
}
