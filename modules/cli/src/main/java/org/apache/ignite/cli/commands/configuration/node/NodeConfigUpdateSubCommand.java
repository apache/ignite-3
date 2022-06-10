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

package org.apache.ignite.cli.commands.configuration.node;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.configuration.NodeConfigUpdateCall;
import org.apache.ignite.cli.call.configuration.NodeConfigUpdateCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command that updates node configuration.
 */
@Command(name = "update",
        description = "Updates node configuration.")
@Singleton
public class NodeConfigUpdateSubCommand extends BaseCommand {
    @Inject
    NodeConfigUpdateCall call;
    /**
     * Node url option.
     */
    @Option(
            names = {"--node-url"}, description = "Url to Ignite node.",
            descriptionKey = "ignite.cluster-url", defaultValue = "http://localhost:10300"
    )
    private String nodeUrl;
    /**
     * Configuration that will be updated.
     */
    @Parameters(index = "0")
    private String config;

    /** {@inheritDoc} */
    @Override
    public void run() {
        CallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .build()
                .runPipeline();
    }

    private NodeConfigUpdateCallInput buildCallInput() {
        return NodeConfigUpdateCallInput.builder()
                .nodeUrl(nodeUrl)
                .config(config)
                .build();
    }
}
