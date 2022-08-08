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

package org.apache.ignite.cli.commands.questions;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Objects;
import org.apache.ignite.cli.call.connect.ConnectCall;
import org.apache.ignite.cli.call.connect.ConnectCallInput;
import org.apache.ignite.cli.config.ConfigManagerProvider;
import org.apache.ignite.cli.core.flow.Flowable;
import org.apache.ignite.cli.core.flow.builder.FlowBuilder;
import org.apache.ignite.cli.core.flow.builder.Flows;
import org.apache.ignite.cli.core.repl.Session;
import org.apache.ignite.cli.core.repl.context.CommandLineContextProvider;


/**
 * Wrapper of command call to question with connection checking.
 */
@Singleton
public class ConnectToClusterQuestion {

    @Inject
    private ConnectCall connectCall;

    @Inject
    private ConfigManagerProvider provider;

    @Inject
    private Session session;


    /**
     * Execute call with question about connect to cluster in case when disconnected state.
     *
     * @param clusterUrl cluster url .
     * @return {@link FlowBuilder} instance with question in case when cluster url.
     */
    public FlowBuilder<Void, String> askQuestionIfNotConnected(String clusterUrl) {
        String clusterProperty = provider.get().getCurrentProperty("ignite.cluster-url");
        String question = "You are not connected to node. Do you want to connect to the default node "
                + clusterProperty + " ? [Y/n] ";

        return Flows.from(clusterUrlOrSessionNode(clusterUrl))
                .ifThen(Objects::isNull, Flows.<String, ConnectCallInput>acceptQuestion(question,
                                () -> new ConnectCallInput(clusterProperty))
                        .then(Flows.fromCall(connectCall))
                        .toOutput(CommandLineContextProvider.getContext())
                        .build())
                .then(prevUrl -> Flowable.success(clusterUrlOrSessionNode(clusterUrl)));
    }

    private String clusterUrlOrSessionNode(String clusterUrl) {
        return clusterUrl != null ? clusterUrl : session.nodeUrl();
    }
}
