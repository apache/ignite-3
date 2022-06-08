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

package org.apache.ignite.cli.commands.connect;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.connect.ConnectCall;
import org.apache.ignite.cli.call.connect.ConnectCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Connects to the Ignite 3 node.
 */
@Command(name = "connect", description = "Connect to Ignite 3 node.")
@Singleton
public class ConnectCommand extends BaseCommand {

    /**
     * Cluster url option.
     */
    @Parameters(
            description = "Ignite node url.",
            descriptionKey = "ignite.cluster-url", defaultValue = "http://localhost:10300"
    )
    private String nodeUrl;

    @Spec
    private CommandSpec spec;

    @Inject
    private ConnectCall connectCall;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(connectCall)
                .inputProvider(() -> ConnectCallInput.builder().nodeUrl(nodeUrl).build())
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }
}
