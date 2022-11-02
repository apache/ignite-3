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

package org.apache.ignite.internal.cli.commands.connect;

import static org.apache.ignite.internal.cli.commands.OptionsConstants.CLUSTER_URL_KEY;
import static org.apache.ignite.internal.cli.commands.OptionsConstants.NODE_NAME_DESC;
import static org.apache.ignite.internal.cli.commands.OptionsConstants.NODE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.OptionsConstants.NODE_NAME_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.OptionsConstants.NODE_URL_DESC;

import jakarta.inject.Inject;
import java.io.File;
import java.net.URL;
import org.apache.ignite.internal.cli.NodeNameRegistry;
import org.apache.ignite.internal.cli.call.connect.ConnectCall;
import org.apache.ignite.internal.cli.call.connect.ConnectCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.converters.UrlConverter;
import org.apache.ignite.internal.cli.deprecated.IgniteCliException;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Connects to the Ignite 3 node.
 */
@Command(name = "connect", description = "Connects to Ignite 3 node")
public class ConnectCommand extends BaseCommand implements Runnable {

    @ArgGroup(multiplicity = "1")
    private ExecOptions execOptions;

    @Inject
    private ConnectCall connectCall;

    @Inject
    private NodeNameRegistry nodeNameRegistry;

    private static class ExecOptions {
        /** Node URL option. */
        @Parameters(description = NODE_URL_DESC, descriptionKey = CLUSTER_URL_KEY, converter = UrlConverter.class)
        private URL nodeUrl;

        @Option(names = {NODE_NAME_OPTION_SHORT, NODE_NAME_OPTION}, description = NODE_NAME_DESC)
        private String nodeName;
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
        CallExecutionPipeline.builder(connectCall)
                .inputProvider(() -> new ConnectCallInput(nodeUrl()))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .verbose(verbose)
                .build()
                .runPipeline();
    }

    private String nodeUrl() {
        if (execOptions.nodeUrl != null) {
            return execOptions.nodeUrl.toString();
        } else {
            String url = nodeNameRegistry.getNodeUrl(execOptions.nodeName);
            if (url != null) {
                return url;
            } else {
                throw new IgniteCliException("Node " + execOptions.nodeName + " not found. Use URL.");
            }
        }
    }
}
