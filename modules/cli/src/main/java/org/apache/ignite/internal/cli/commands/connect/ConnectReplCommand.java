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

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_KEY;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_URL_OPTION_DESC;

import jakarta.inject.Inject;
import java.net.URL;
import org.apache.ignite.internal.cli.call.connect.ConnectCallInput;
import org.apache.ignite.internal.cli.call.connect.ConnectWizardCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.converters.UrlConverter;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Connects to the Ignite 3 node in REPL mode.
 */
@Command(name = "connect", description = "Connects to Ignite 3 node")
public class ConnectReplCommand extends BaseCommand implements Runnable {

    /** Node URL option. */
    @Parameters(description = NODE_URL_OPTION_DESC, descriptionKey = CLUSTER_URL_KEY, converter = UrlConverter.class)
    private URL nodeUrl;

    @ArgGroup(exclusive = false)
    private ConnectOptions connectOptions;

    @Inject
    private ConnectWizardCall connectCall;

    @Inject
    private ConnectToClusterQuestion question;

    /** {@inheritDoc} */
    @Override
    public void run() {
        question.askQuestionIfConnected(connectCallInput(nodeUrl.toString()))
                .then(Flows.fromCall(connectCall))
                .verbose(verbose)
                .print()
                .start();
    }

    private ConnectCallInput connectCallInput(String nodeUrl) {
        return ConnectCallInput.builder()
                .url(nodeUrl)
                .username(connectOptions != null ? connectOptions.username() : null)
                .password(connectOptions != null ? connectOptions.password() : null)
                .build();
    }
}
