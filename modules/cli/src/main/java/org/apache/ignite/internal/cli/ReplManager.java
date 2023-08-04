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

package org.apache.ignite.internal.cli;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.commands.TopLevelCliReplCommand;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.call.StringCallInput;
import org.apache.ignite.internal.cli.core.exception.handler.DefaultExceptionHandlers;
import org.apache.ignite.internal.cli.core.repl.EventListeningActivationPoint;
import org.apache.ignite.internal.cli.core.repl.Repl;
import org.apache.ignite.internal.cli.core.repl.SessionDefaultValueProvider;
import org.apache.ignite.internal.cli.core.repl.executor.ReplExecutorProvider;
import org.apache.ignite.internal.cli.core.repl.prompt.PromptProvider;

/**
 * Class which runs main REPL mode, it's used both when starting directly into the REPL mode and from the `connect` command.
 */
@Singleton
public class ReplManager {
    @Inject
    private ReplExecutorProvider replExecutorProvider;

    @Inject
    private PromptProvider promptProvider;

    @Inject
    private SessionDefaultValueProvider defaultValueProvider;

    @Inject
    private ConnectToClusterQuestion question;

    @Inject
    private EventListeningActivationPoint eventListeningActivationPoint;

    /**
     * Enters REPL mode.
     */
    public void startReplMode() {
        replExecutorProvider.get().execute(Repl.builder()
                .withPromptProvider(promptProvider)
                .withCommandClass(TopLevelCliReplCommand.class)
                .withDefaultValueProvider(defaultValueProvider)
                .withCallExecutionPipelineProvider((executor, exceptionHandlers, line) ->
                        CallExecutionPipeline.builder(executor)
                                .inputProvider(() -> new StringCallInput(line))
                                .output(System.out)
                                .errOutput(System.err)
                                .exceptionHandlers(new DefaultExceptionHandlers())
                                .exceptionHandlers(exceptionHandlers)
                                .build())
                .withOnStart(question::askQuestionOnReplStart)
                .withHistoryFileName("history")
                .withAutosuggestionsWidgets()
                .withEventSubscriber(eventListeningActivationPoint)
                .build());
    }
}
