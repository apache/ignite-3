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

package org.apache.ignite.internal.cli.commands.cluster.config;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigUpdateCall;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigUpdateCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.SpacedParameterMixin;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlProfileMixin;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/**
 * Command that updates cluster configuration.
 */
@Command(name = "update", description = "Updates cluster configuration")
public class ClusterConfigUpdateCommand extends BaseCommand implements Callable<Integer> {
    /** Cluster endpoint URL option. */
    @Mixin
    private ClusterUrlProfileMixin clusterUrl;

    /** Configuration that will be updated. */
    @Mixin
    private SpacedParameterMixin config;

    @Inject
    ClusterConfigUpdateCall call;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .exceptionHandler(new ClusterNotInitializedExceptionHandler(
                        "Cannot update cluster config", "ignite cluster init"
                ))
                .verbose(verbose)
                .build()
                .runPipeline();
    }

    private ClusterConfigUpdateCallInput buildCallInput() {
        return ClusterConfigUpdateCallInput.builder()
                .clusterUrl(clusterUrl.getClusterUrl())
                .config(config.toString())
                .build();
    }
}
