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

package org.apache.ignite.cli.commands.configuration.cluster;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.configuration.ClusterConfigUpdateCall;
import org.apache.ignite.cli.call.configuration.ClusterConfigUpdateCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.cli.core.flow.Flowable;
import org.apache.ignite.cli.core.repl.Session;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command that updates cluster configuration in REPL mode.
 */
@Command(name = "update",
        description = "Updates cluster configuration.")
@Singleton
public class ClusterConfigUpdateReplSubCommand extends BaseCommand implements Runnable {
    /**
     * Cluster url option.
     */
    @Option(
            names = {"--cluster-url"}, description = "Url to Ignite node.",
            descriptionKey = "ignite.cluster-url", defaultValue = "http://localhost:10300"
    )
    private String clusterUrl;

    /**
     * Configuration that will be updated.
     */
    @Parameters(index = "0")
    private String config;

    @Inject
    ClusterConfigUpdateCall call;

    @Inject
    private Session session;

    @Inject
    private ConnectToClusterQuestion question;

    /** {@inheritDoc} */
    @Override
    public void run() {
        question.callWithConnectQuestion(spec,
                        this::getClusterUrl,
                        s -> ClusterConfigUpdateCallInput.builder().config(config).clusterUrl(getClusterUrl()).build(),
                        call)
                .build()
                .call(Flowable.empty());
    }

    private String getClusterUrl() {
        String s = null;
        if (session.isConnectedToNode()) {
            s = session.getNodeUrl();
        } else if (clusterUrl != null) {
            s = clusterUrl;
        }
        return s;
    }
}
