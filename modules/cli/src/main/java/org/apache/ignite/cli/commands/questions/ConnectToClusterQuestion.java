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
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.cli.call.connect.ConnectCall;
import org.apache.ignite.cli.call.connect.ConnectCallInput;
import org.apache.ignite.cli.config.ConfigManagerProvider;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallInput;
import org.apache.ignite.cli.core.flow.Flowable;
import org.apache.ignite.cli.core.flow.Flows;
import org.apache.ignite.cli.core.flow.builder.FlowBuilder;
import org.apache.ignite.cli.core.flow.question.AcceptedQuestionAnswer;
import org.apache.ignite.cli.core.flow.question.InterruptQuestionAnswer;
import picocli.CommandLine.Model.CommandSpec;


/**
 * Wrapper of command call to question with connection checking.
 */
@Singleton
public class ConnectToClusterQuestion {

    @Inject
    private ConnectCall connectCall;

    @Inject
    private ConfigManagerProvider provider;


    /**
     * Execute call with question about connect to cluster in case when disconnected state.
     *
     * @param spec command spec.
     * @param clusterUrl cluster url provider.
     * @param clusterUrlMapper mapper of cluster url to call input
     * @param call call instance.
     * @param <T> call input type.
     * @param <O> call output type.
     * @return {@link FlowBuilder} instance with question and provided call.
     */
    public <T extends CallInput, O> FlowBuilder<Void, O> callWithConnectQuestion(
            CommandSpec spec,
            Supplier<String> clusterUrl,
            Function<String, T> clusterUrlMapper,
            Call<T, O> call) {
        String question = "You are not connected to node. Do you want to connect to the default node "
                + provider.get().getCurrentProperty("ignite.cluster-url") + " ? [Y/n] ";

        return Flows.from(clusterUrl.get())
                .ifThen(Objects::isNull, Flows.<String, ConnectCallInput>question(question,
                                List.of(
                                        new AcceptedQuestionAnswer<>((a, i) ->
                                                new ConnectCallInput(provider.get().getCurrentProperty("ignite.cluster-url"))),
                                        new InterruptQuestionAnswer<>())
                        ).appendFlow(Flows.fromCall(connectCall))
                        .toOutput(spec.commandLine().getOut(), spec.commandLine().getErr())
                        .build())
                .ifThen(s -> Objects.isNull(clusterUrl.get()), input -> Flowable.interrupt())
                .map(clusterUrlMapper)
                .appendFlow(Flows.fromCall(call))
                .toOutput(spec.commandLine().getOut(), spec.commandLine().getErr());
    }
}


