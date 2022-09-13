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

package org.apache.ignite.cli.commands.node.version;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.node.version.NodeVersionCall;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.node.NodeUrlMixin;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.StringCallInput;
import org.apache.ignite.cli.core.repl.Session;
import picocli.CommandLine;

import java.util.concurrent.Callable;

import static org.apache.ignite.cli.core.style.component.CommonMessages.CONNECT_OR_USE_NODE_URL_MESSAGE;

/**
 * Display the node version in REPL.
 */
@CommandLine.Command(name = "version", description = "Prints the node build version")
public class NodeVersionReplCommand extends BaseCommand implements Callable<Integer> {
    /**
     * Node URL option.
     */
    @CommandLine.Mixin
    private NodeUrlMixin nodeUrl;

    @Inject
    private NodeVersionCall nodeVersionCall;

    @Inject
    private Session session;

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer call() {
        String inputUrl;

        if (nodeUrl.getNodeUrl() != null) {
            inputUrl = nodeUrl.getNodeUrl();
        } else if (session.isConnectedToNode()) {
            inputUrl = session.nodeUrl();
        } else {
            spec.commandLine().getErr().println(CONNECT_OR_USE_NODE_URL_MESSAGE.render());
            return 2;
        }

        return CallExecutionPipeline.builder(nodeVersionCall)
                .inputProvider(() -> new StringCallInput(inputUrl))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }
}
