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

package org.apache.ignite.cli.commands.node.status;

import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_KEY;
import static org.apache.ignite.cli.commands.OptionsConstants.NODE_URL_DESC;
import static org.apache.ignite.cli.commands.OptionsConstants.NODE_URL_OPTION;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.call.node.status.NodeStatusCall;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.decorators.NodeStatusDecorator;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.StatusCallInput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Display the node status.
 */
@Command(name = "status",
        description = "Prints status of the node.")
@Singleton
public class NodeStatusSubCommand extends BaseCommand implements Callable<Integer> {

    /**
     * Node URL option.
     */
    @SuppressWarnings("PMD.UnusedPrivateField")
    @Option(names = {NODE_URL_OPTION}, description = NODE_URL_DESC, descriptionKey = CLUSTER_URL_KEY)
    private String nodeUrl;

    @Inject
    private NodeStatusCall nodeStatusCall;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(nodeStatusCall)
                .inputProvider(() -> new StatusCallInput(nodeUrl))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new NodeStatusDecorator())
                .build()
                .runPipeline();
    }
}
