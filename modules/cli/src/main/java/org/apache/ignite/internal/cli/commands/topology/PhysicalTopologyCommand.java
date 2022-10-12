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

package org.apache.ignite.internal.cli.commands.topology;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.cluster.topology.PhysicalTopologyCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlProfileMixin;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.decorators.PlainTopologyDecorator;
import org.apache.ignite.internal.cli.decorators.TopologyDecorator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Command that show physical cluster topology.
 */
@Command(name = "physical")
public class PhysicalTopologyCommand extends BaseCommand implements Callable<Integer> {
    /** Cluster endpoint URL option. */
    @Mixin
    private ClusterUrlProfileMixin clusterUrl;

    @Inject
    private PhysicalTopologyCall call;

    @Option(names = "--plain", description = "Display output with plain formatting")
    private boolean plain;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        TopologyDecorator topologyDecorator = plain ? new PlainTopologyDecorator() : new TopologyDecorator();
        return CallExecutionPipeline.builder(call)
                .inputProvider(() -> new UrlCallInput(clusterUrl.getClusterUrl()))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(topologyDecorator)
                .build()
                .runPipeline();
    }
}
