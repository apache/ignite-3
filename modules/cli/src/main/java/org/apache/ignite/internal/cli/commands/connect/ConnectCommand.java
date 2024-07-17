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

import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_URL_OPTION_DESC;

import jakarta.inject.Inject;
import java.net.URL;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.ReplManager;
import org.apache.ignite.internal.cli.call.connect.ConnectCall;
import org.apache.ignite.internal.cli.call.connect.ConnectCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.converters.UrlConverter;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Connects to the Ignite 3 node.
 */
@Command(name = "connect", description = "Connects to Ignite 3 node")
public class ConnectCommand extends BaseCommand implements Callable<Integer> {

    /** Node URL option. */
    @Parameters(description = NODE_URL_OPTION_DESC, converter = UrlConverter.class)
    private URL nodeUrl;

    @ArgGroup(exclusive = false)
    private ConnectOptions connectOptions;

    @Inject
    private ConnectCall connectCall;

    @Inject
    private ReplManager replManager;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        int exitCode = CallExecutionPipeline.builder(connectCall)
                .inputProvider(this::connectCallInput)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .verbose(verbose)
                .build()
                .runPipeline();
        if (exitCode == 0) {
            replManager.startReplMode();
        }
        return exitCode;
    }

    private ConnectCallInput connectCallInput() {
        return ConnectCallInput.builder()
                .url(nodeUrl.toString())
                .username(connectOptions != null ? connectOptions.username() : null)
                .password(connectOptions != null ? connectOptions.password() : null)
                .build();
    }
}
