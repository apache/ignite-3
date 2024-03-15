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

import java.util.Map;
import org.apache.ignite.internal.cli.core.CallExecutionPipelineProvider;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.internal.cli.core.repl.executor.RegistryCommandExecutor;
import org.apache.ignite.internal.cli.core.repl.prompt.PromptProvider;
import org.apache.ignite.internal.cli.core.repl.terminal.TerminalCustomizer;
import org.jline.reader.Completer;
import org.jline.reader.Highlighter;
import org.jline.reader.Parser;
import org.jline.terminal.Terminal;
import picocli.CommandLine.IDefaultValueProvider;

/**
 * Data class with all information about REPL.
 */
public class Repl {

    private final PromptProvider promptProvider;

    private final Map<String, String> aliases;

    private final TerminalCustomizer terminalCustomizer;

    private final Class<?> commandClass;

    private final IDefaultValueProvider defaultValueProvider;

    private final CallExecutionPipelineProvider provider;

    private final Completer completer;

    private final String historyFileName;

    private final boolean tailTipWidgetsEnabled;

    private final boolean autosuggestionsWidgetsEnabled;

    private final Runnable onStart;

    private final EventListeningActivationPoint eventListeningActivationPoint;

    private final Parser parser;

    private Highlighter higilighter;

    /**
     * Constructor.
     *
     * @param promptProvider REPL prompt provider.
     * @param commandClass top level command class.
     * @param defaultValueProvider default value provider.
     * @param aliases map of aliases for commands.
     * @param terminalCustomizer customizer of terminal.
     * @param provider default call execution pipeline provider.
     * @param completer completer instance.
     * @param historyFileName file name for storing commands history.
     * @param tailTipWidgetsEnabled whether tailtip widgets are enabled.
     * @param onStart callback that will run when REPL is started.
     * @param eventListeningActivationPoint event listening activation point
     * @param highlighter syntax highilighter.
     */
    public Repl(PromptProvider promptProvider,
            Class<?> commandClass,
            IDefaultValueProvider defaultValueProvider,
            Map<String, String> aliases,
            TerminalCustomizer terminalCustomizer,
            CallExecutionPipelineProvider provider,
            Completer completer,
            String historyFileName,
            boolean tailTipWidgetsEnabled,
            boolean autosuggestionsWidgetsEnabled,
            Runnable onStart,
            EventListeningActivationPoint eventListeningActivationPoint,
            Highlighter highlighter,
            Parser parser) {
        this.promptProvider = promptProvider;
        this.commandClass = commandClass;
        this.defaultValueProvider = defaultValueProvider;
        this.aliases = aliases;
        this.terminalCustomizer = terminalCustomizer;
        this.provider = provider;
        this.completer = completer;
        this.historyFileName = historyFileName;
        this.tailTipWidgetsEnabled = tailTipWidgetsEnabled;
        this.autosuggestionsWidgetsEnabled = autosuggestionsWidgetsEnabled;
        this.onStart = onStart;
        this.eventListeningActivationPoint = eventListeningActivationPoint;
        this.higilighter = highlighter;
        this.parser = parser;
    }

    /**
     * Builder provider method.
     *
     * @return new instance of builder {@link ReplBuilder}.
     */
    public static ReplBuilder builder() {
        return new ReplBuilder();
    }

    public PromptProvider getPromptProvider() {
        return promptProvider;
    }

    /**
     * Getter for {@code commandClass} field.
     *
     * @return class with top level command.
     */
    public Class<?> commandClass() {
        return commandClass;
    }

    /**
     * Getter for {@code defaultValueProvider} field.
     *
     * @return default value provider.
     */
    public IDefaultValueProvider defaultValueProvider() {
        return defaultValueProvider;
    }

    /**
     * Getter for {@code aliases} field.
     *
     * @return map of command aliases.
     */
    public Map<String, String> getAliases() {
        return aliases;
    }

    /**
     * Method for {@param terminal} customization.
     */
    public void customizeTerminal(Terminal terminal) {
        terminalCustomizer.customize(terminal);
    }

    public CallExecutionPipeline<?, ?> getPipeline(RegistryCommandExecutor executor, ExceptionHandlers exceptionHandlers, String line) {
        return provider.get(executor, exceptionHandlers, line);
    }

    public Completer getCompleter() {
        return completer;
    }

    public String getHistoryFileName() {
        return historyFileName;
    }

    public boolean isTailTipWidgetsEnabled() {
        return tailTipWidgetsEnabled;
    }

    public boolean isAutosuggestionsWidgetsEnabled() {
        return autosuggestionsWidgetsEnabled;
    }

    public Highlighter getHighlighter() {
        return higilighter;
    }

    public void onStart() {
        onStart.run();
    }

    public EventListeningActivationPoint getEventListeningActivationPoint() {
        return eventListeningActivationPoint;
    }

    public Parser getParser() {
        return parser;
    }
}
