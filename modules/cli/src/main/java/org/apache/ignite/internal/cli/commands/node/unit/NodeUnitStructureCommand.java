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

package org.apache.ignite.internal.cli.commands.node.unit;

import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.UNIT_VERSION_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERSION_OPTION;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.node.unit.NodeUnitStructureCall;
import org.apache.ignite.internal.cli.call.unit.UnitStructureCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.node.NodeUrlProfileMixin;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.decorators.UnitStructureDecorator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/** Command to show deployment unit structure. */
@Command(name = "structure", description = "Shows the structure of a deployed unit")
public class NodeUnitStructureCommand extends BaseCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Deployment unit id")
    private String unitId;

    @Option(names = VERSION_OPTION, description = UNIT_VERSION_OPTION_DESC, required = true)
    private String version;

    @Mixin
    private NodeUrlProfileMixin nodeUrl;

    @Option(names = PLAIN_OPTION, description = PLAIN_OPTION_DESC)
    private boolean plain;

    @Inject
    private NodeUnitStructureCall call;

    @Override
    public Integer call() throws Exception {
        return runPipeline(CallExecutionPipeline.builder(call)
                .inputProvider(() -> UnitStructureCallInput.builder()
                        .unitId(unitId)
                        .version(version)
                        .url(nodeUrl.getNodeUrl())
                        .build())
                .decorator(new UnitStructureDecorator(plain))
                .exceptionHandler(ClusterNotInitializedExceptionHandler.createHandler("Cannot get unit structure"))
        );
    }
}
