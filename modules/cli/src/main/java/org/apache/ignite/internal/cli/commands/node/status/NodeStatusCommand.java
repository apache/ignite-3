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

package org.apache.ignite.internal.cli.commands.node.status;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.node.status.NodeStatusCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.node.NodeUrlProfileMixin;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.decorators.NodeStatusDecorator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/**
 * Display the node status.
 */
@Command(name = "status", description = "Prints status of the node")
public class NodeStatusCommand extends BaseCommand implements Callable<Integer> {
    /** Node URL option. */
    @Mixin
    private NodeUrlProfileMixin nodeUrl;

    @Inject
    private NodeStatusCall call;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(call)
                .inputProvider(() -> new UrlCallInput(nodeUrl.getNodeUrl()))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new NodeStatusDecorator())
                .verbose(verbose)
                .build()
                .runPipeline();
    }
}
