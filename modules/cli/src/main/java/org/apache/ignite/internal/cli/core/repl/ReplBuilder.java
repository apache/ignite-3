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

package org.apache.ignite.internal.cli.core.repl;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.cli.core.CallExecutionPipelineProvider;
import org.apache.ignite.internal.cli.core.repl.prompt.PromptProvider;
import org.apache.ignite.internal.cli.core.repl.terminal.TerminalCustomizer;
import org.jline.reader.Completer;
import org.jline.reader.Highlighter;
import org.jline.reader.Parser;
import picocli.CommandLine.IDefaultValueProvider;

/**
 * Builder of {@link Repl}.
 */
public class ReplBuilder {

    private PromptProvider promptProvider;

    private final Map<String, String> aliases = new HashMap<>();

    private Class<?> commandClass;

    private IDefaultValueProvider defaultValueProvider;

    private TerminalCustomizer terminalCustomizer = terminal -> {
    };

    private Completer completer;

    private CallExecutionPipelineProvider provider;

    private String historyFileName;

    private boolean tailTipWidgetsEnabled;

    private boolean autosuggestionsWidgetsEnabled;

    private Runnable onStart = () -> {};

    private EventListeningActivationPoint eventListeningActivationPoint;

    private Highlighter highlighter;

    private Parser parser;

    /**
     * Build methods.
     *
     * @return new instance of {@link Repl}.
     */
    public Repl build() {
        return new Repl(
                promptProvider,
                commandClass,
                defaultValueProvider,
                aliases,
                terminalCustomizer,
                provider,
                completer,
                historyFileName,
                tailTipWidgetsEnabled,
                autosuggestionsWidgetsEnabled,
                onStart,
                eventListeningActivationPoint,
                highlighter,
                parser
        );
    }

    public ReplBuilder withPromptProvider(PromptProvider promptProvider) {
        this.promptProvider = promptProvider;
        return this;
    }

    /**
     * Builder setter of {@code commandClass} field.
     *
     * @param commandClass class with top level command.
     * @return invoked builder instance {@link ReplBuilder}.
     */
    public ReplBuilder withCommandClass(Class<?> commandClass) {
        this.commandClass = commandClass;
        return this;
    }

    /**
     * Builder setter of {@code defaultValueProvider} field.
     *
     * @param defaultValueProvider default value provider.
     * @return invoked builder instance {@link ReplBuilder}.
     */
    public ReplBuilder withDefaultValueProvider(IDefaultValueProvider defaultValueProvider) {
        this.defaultValueProvider = defaultValueProvider;
        return this;
    }

    /**
     * Builder setter of {@code aliases} field.
     *
     * @param aliases map of aliases for commands.
     * @return invoked builder instance {@link ReplBuilder}.
     */
    public ReplBuilder withAliases(Map<String, String> aliases) {
        this.aliases.putAll(aliases);
        return this;
    }

    /**
     * Builder setter of {@code terminalCustomizer} field.
     *
     * @param terminalCustomizer customizer of terminal {@link org.jline.terminal.Terminal}.
     * @return invoked builder instance {@link ReplBuilder}.
     */
    public ReplBuilder withTerminalCustomizer(TerminalCustomizer terminalCustomizer) {
        this.terminalCustomizer = terminalCustomizer;
        return this;
    }

    public ReplBuilder withCompleter(Completer completer) {
        this.completer = completer;
        return this;
    }

    public ReplBuilder withCallExecutionPipelineProvider(CallExecutionPipelineProvider provider) {
        this.provider = provider;
        return this;
    }

    public ReplBuilder withOnStart(Runnable onStart) {
        this.onStart = onStart;
        return this;
    }

    public ReplBuilder withHistoryFileName(String historyFileName) {
        this.historyFileName = historyFileName;
        return this;
    }

    public ReplBuilder withTailTipWidgets() {
        this.tailTipWidgetsEnabled = true;
        return this;
    }

    public ReplBuilder withAutosuggestionsWidgets() {
        this.autosuggestionsWidgetsEnabled = true;
        return this;
    }

    /**
     * Builder setter of {@code eventListeningActivationPoint} field.
     *
     * @param eventListeningActivationPoint event listening activation point.
     * @return invoked builder instance {@link ReplBuilder}.
     */
    public ReplBuilder withEventSubscriber(EventListeningActivationPoint eventListeningActivationPoint) {
        this.eventListeningActivationPoint = eventListeningActivationPoint;
        return this;
    }

    public ReplBuilder withHighlighter(Highlighter highlighter) {
        this.highlighter = highlighter;
        return this;
    }

    public ReplBuilder withParser(Parser parser) {
        this.parser = parser;
        return this;
    }
}
