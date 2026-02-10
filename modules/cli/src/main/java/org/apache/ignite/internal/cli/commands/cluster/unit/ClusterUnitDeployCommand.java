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

package org.apache.ignite.internal.cli.commands.cluster.unit;

import static org.apache.ignite.internal.cli.core.call.CallExecutionPipeline.asyncBuilder;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.cluster.unit.DeployUnitCallFactory;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlProfileMixin;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/** Command to deploy a unit. */
@Command(name = "deploy", description = "Deploys a unit from file or a directory (use --recursive for subdirectories)")
public class ClusterUnitDeployCommand extends BaseCommand implements Callable<Integer> {

    @Mixin
    private ClusterUrlProfileMixin clusterUrl;

    @Mixin
    private UnitDeployOptionsMixin options;

    @Inject
    private DeployUnitCallFactory callFactory;

    @Override
    public Integer call() throws Exception {
        return runPipeline(asyncBuilder(callFactory)
                .inputProvider(() -> options.toDeployUnitCallInput(clusterUrl.getClusterUrl()))
                .exceptionHandler(ClusterNotInitializedExceptionHandler.createHandler("Cannot deploy unit"))
        );
    }
}
