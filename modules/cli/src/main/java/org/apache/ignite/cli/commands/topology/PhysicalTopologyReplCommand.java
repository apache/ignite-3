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

package org.apache.ignite.cli.commands.topology;

import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_DESC;
import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_KEY;
import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_OPTION;
import static org.apache.ignite.cli.core.style.component.CommonMessages.CONNECT_OR_USE_NODE_URL_MESSAGE;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.call.cluster.topology.PhysicalTopologyCall;
import org.apache.ignite.cli.call.cluster.topology.TopologyCallInput;
import org.apache.ignite.cli.call.cluster.topology.TopologyCallInput.TopologyCallInputBuilder;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.repl.Session;
import org.apache.ignite.cli.decorators.TopologyDecorator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command that show physical cluster topology in REPL mode.
 */
@Command(name = "physical")
public class PhysicalTopologyReplCommand extends BaseCommand implements Callable<Integer> {
    /** Cluster endpoint URL option. */
    @Option(names = {CLUSTER_URL_OPTION}, description = CLUSTER_URL_DESC, descriptionKey = CLUSTER_URL_KEY)
    private String clusterUrl;

    @Inject
    private PhysicalTopologyCall call;

    @Inject
    private Session session;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        TopologyCallInputBuilder inputBuilder = TopologyCallInput.builder();

        if (clusterUrl != null) {
            inputBuilder.clusterUrl(clusterUrl);
        } else if (session.isConnectedToNode()) {
            inputBuilder.clusterUrl(session.nodeUrl());
        } else {
            spec.commandLine().getErr().println(CONNECT_OR_USE_NODE_URL_MESSAGE.render());
            return 2;
        }

        return CallExecutionPipeline.builder(call)
                .inputProvider(inputBuilder::build)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new TopologyDecorator())
                .build()
                .runPipeline();
    }
}
