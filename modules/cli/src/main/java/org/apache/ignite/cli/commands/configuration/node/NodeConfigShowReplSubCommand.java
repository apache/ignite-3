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
import org.apache.ignite.cli.call.configuration.NodeConfigShowCall;
import org.apache.ignite.cli.call.configuration.NodeConfigShowCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.cli.core.flow.Flowable;
import org.apache.ignite.cli.core.repl.Session;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command that shows node configuration from the cluster in REPL mode.
 */
@Command(name = "show",
        description = "Shows node configuration.")
public class NodeConfigShowReplSubCommand extends BaseCommand implements Runnable {

    /**
     * Configuration selector option.
     */
    @Option(names = {"--selector"}, description = "Configuration path selector.")
    private String selector;

    /**
     * Node url option.
     */
    @Option(
            names = {"--node-url"}, description = "Url to Ignite node."
    )
    private String nodeUrl;

    @Inject
    private NodeConfigShowCall call;

    @Inject
    private Session session;

    @Inject
    private ConnectToClusterQuestion question;

    /** {@inheritDoc} */
    @Override
    public void run() {
        question.callWithConnectQuestion(spec,
                        this::getNodeUrl,
                        s -> NodeConfigShowCallInput.builder().selector(selector).nodeUrl(getNodeUrl()).build(),
                        call)
                .build()
                .call(Flowable.empty());
    }

    private String getNodeUrl() {
        String s = null;
        if (session.isConnectedToNode()) {
            s = session.getNodeUrl();
        } else if (nodeUrl != null) {
            s = nodeUrl;
        }
        return s;
    }
}
