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

package org.apache.ignite.internal.cli.commands.unit;


import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.unit.UnitStatusCall;
import org.apache.ignite.internal.cli.call.unit.UnitStatusCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlProfileMixin;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.decorators.UnitStatusDecorator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

/** Command to show status of the unit. */
@Command(name = "status", description = "Shows status of the unit")
public class UnitStatusCommand extends BaseCommand implements Callable<Integer> {

    @Mixin
    private ClusterUrlProfileMixin clusterUrl;

    /** Unit id. */
    @Parameters(index = "0")
    private String id;

    @Inject
    private UnitStatusCall statusCall;

    @Override
    public Integer call() throws Exception {
        return CallExecutionPipeline.builder(statusCall)
                .inputProvider(() ->
                        UnitStatusCallInput.builder()
                                .id(id)
                                .clusterUrl(clusterUrl.getClusterUrl())
                                .build())
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .verbose(verbose)
                .decorator(new UnitStatusDecorator())
                .exceptionHandler(new ClusterNotInitializedExceptionHandler("Cannot get unit status", "ignite cluster init"))
                .build().runPipeline();
    }
}
