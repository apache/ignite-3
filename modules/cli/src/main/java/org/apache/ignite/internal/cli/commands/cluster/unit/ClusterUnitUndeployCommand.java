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


import static org.apache.ignite.internal.cli.commands.Options.Constants.UNIT_VERSION_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERSION_OPTION;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.cluster.unit.UndeployUnitCall;
import org.apache.ignite.internal.cli.call.cluster.unit.UndeployUnitCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlProfileMixin;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/** Command to undeploy a unit. */
@Command(name = "undeploy", description = "Undeploys a unit")
public class ClusterUnitUndeployCommand extends BaseCommand implements Callable<Integer> {

    @Mixin
    private ClusterUrlProfileMixin clusterUrl;

    /** Unit id. */
    @Parameters(index = "0", description = "Unit id")
    private String id;

    /** Unit version. */
    @Option(names = VERSION_OPTION, description = UNIT_VERSION_OPTION_DESC, required = true)
    private String version;

    @Inject
    private UndeployUnitCall call;

    @Override
    public Integer call() throws Exception {
        return runPipeline(CallExecutionPipeline.builder(call)
                .inputProvider(() -> UndeployUnitCallInput.builder()
                        .id(id)
                        .version(version)
                        .clusterUrl(clusterUrl.getClusterUrl())
                        .build())
                .exceptionHandler(ClusterNotInitializedExceptionHandler.createHandler("Cannot undeploy unit"))
        );
    }
}
