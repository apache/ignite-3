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

package org.apache.ignite.internal.cli.commands.cluster;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterRenameCall;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterRenameCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/**
 * Command that renames the cluster.
 */
@Command(name = "rename", description = "Renames the cluster")
public class ClusterRenameCommand extends BaseCommand implements Callable<Integer> {
    /** Cluster endpoint URL option. */
    @Mixin
    private ClusterUrlMixin clusterUrl;

    /** Name that will be updated. */
    @Mixin
    private ClusterNameMixin nameFromArgs;

    @Inject
    ClusterRenameCall call;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        return runPipeline(CallExecutionPipeline.builder(call)
                .input(buildCallInput())
                .exceptionHandler(createHandler("Cannot update cluster config"))
        );
    }

    private ClusterRenameCallInput buildCallInput() {
        return ClusterRenameCallInput.builder()
                .clusterUrl(clusterUrl.getClusterUrl())
                .name(nameFromArgs.getName())
                .build();
    }
}
