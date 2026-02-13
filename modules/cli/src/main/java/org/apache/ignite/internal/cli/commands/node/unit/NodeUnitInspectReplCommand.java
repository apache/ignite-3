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

package org.apache.ignite.internal.cli.commands.node.unit;

import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.UNIT_VERSION_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERSION_OPTION;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.node.unit.NodeUnitInspectCall;
import org.apache.ignite.internal.cli.call.unit.UnitInspectCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.node.NodeUrlMixin;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import org.apache.ignite.internal.cli.decorators.UnitInspectDecorator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/** Command to inspect deployment unit in REPL mode. */
@Command(name = "inspect", description = "Inspects the structure of a deployed unit")
public class NodeUnitInspectReplCommand extends BaseCommand implements Runnable {

    @Parameters(index = "0", description = "Deployment unit id")
    private String unitId;

    @Option(names = VERSION_OPTION, description = UNIT_VERSION_OPTION_DESC, required = true)
    private String version;

    @Mixin
    private NodeUrlMixin nodeUrl;

    @Option(names = PLAIN_OPTION, description = PLAIN_OPTION_DESC)
    private boolean plain;

    @Inject
    private NodeUnitInspectCall call;

    @Inject
    private ConnectToClusterQuestion question;

    @Override
    public void run() {
        runFlow(question.askQuestionIfNotConnected(nodeUrl.getNodeUrl())
                .map(url -> UnitInspectCallInput.builder()
                        .unitId(unitId)
                        .version(version)
                        .url(url)
                        .build())
                .then(Flows.fromCall(call))
                .exceptionHandler(ClusterNotInitializedExceptionHandler.createReplHandler("Cannot inspect unit"))
                .print(new UnitInspectDecorator(plain))
        );
    }
}
