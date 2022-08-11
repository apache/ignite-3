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

package org.apache.ignite.cli.commands.node.config;

import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_KEY;
import static org.apache.ignite.cli.commands.OptionsConstants.NODE_URL_DESC;
import static org.apache.ignite.cli.commands.OptionsConstants.NODE_URL_OPTION;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.call.configuration.NodeConfigShowCall;
import org.apache.ignite.cli.call.configuration.NodeConfigShowCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.decorators.JsonDecorator;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command that shows configuration from the cluster.
 */
@Command(name = "show",
        description = "Shows node configuration.")
public class NodeConfigShowSubCommand extends BaseCommand implements Callable<Integer> {

    /**
     * Configuration selector option.
     */
    @Option(names = {"--selector"}, description = "Configuration path selector.")
    private String selector;

    /**
     * Node URL option.
     */
    @Option(names = {NODE_URL_OPTION}, description = NODE_URL_DESC, descriptionKey = CLUSTER_URL_KEY)
    private String nodeUrl;

    @Inject
    private NodeConfigShowCall call;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new JsonDecorator())
                .build()
                .runPipeline();
    }

    private NodeConfigShowCallInput buildCallInput() {
        return NodeConfigShowCallInput.builder()
                .nodeUrl(nodeUrl)
                .selector(selector)
                .build();
    }
}
