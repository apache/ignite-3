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

package org.apache.ignite.internal.cli.core.repl.executor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.config.StateFolderProvider;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.internal.cli.core.exception.handler.PicocliExecutionExceptionHandler;
import org.apache.ignite.internal.cli.core.exception.handler.ReplExceptionHandlers;
import org.apache.ignite.internal.cli.core.repl.Repl;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleterActivationPoint;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleterRegistry;
import org.apache.ignite.internal.cli.core.repl.completer.filter.CompleterFilter;
import org.apache.ignite.internal.cli.core.repl.completer.filter.DeployUnitsOptionsFilter;
import org.apache.ignite.internal.cli.core.repl.completer.filter.DynamicCompleterFilter;
import org.apache.ignite.internal.cli.core.repl.completer.filter.NonRepeatableOptionsFilter;
import org.apache.ignite.internal.cli.core.repl.completer.filter.ShortOptionsFilter;
import org.apache.ignite.internal.cli.core.repl.context.CommandLineContext;
import org.apache.ignite.internal.cli.core.repl.context.CommandLineContextProvider;
import org.apache.ignite.internal.cli.core.repl.expander.NoopExpander;
import org.apache.ignite.internal.cli.core.repl.terminal.PagerSupport;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.reader.Completer;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.reader.LineReader.SuggestionType;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.Parser;
import org.jline.reader.impl.DefaultHighlighter;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.widget.AutosuggestionWidgets;
import org.jline.widget.TailTipWidgets;
import picocli.CommandLine;
import picocli.CommandLine.IDefaultValueProvider;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

/**
 * Executor of {@link Repl}.
 */
public class ReplExecutorImpl implements ReplExecutor {

    /** Maximum number of tab completion candidates to display. */
    private static final int MAX_TAB_COMPLETION_CANDIDATES = 50;

    /** Number of lines for tail tip widget descriptions. */
    private static final int TAIL_TIP_DESCRIPTION_LINES = 5;

    private static final Supplier<Path> WORK_DIR_PROVIDER = () -> Paths.get(System.getProperty("user.dir"));

    private final Parser parser = new DefaultParser().escapeChars(null);

    private final AtomicBoolean interrupted = new AtomicBoolean();

    private final ExceptionHandlers exceptionHandlers = new ReplExceptionHandlers(interrupted::set);

    private final PicocliCommandsFactory factory;

    private final Terminal terminal;

    private final PagerSupport pagerSupport;

    private TailTipWidgets tailTipWidgets;

    /**
     * Constructor.
     *
     * @param commandsFactory picocli commands factory.
     * @param terminal terminal instance.
     * @param configManagerProvider configuration manager provider for pager settings.
     */
    public ReplExecutorImpl(PicocliCommandsFactory commandsFactory, Terminal terminal, ConfigManagerProvider configManagerProvider) {
        this.factory = commandsFactory;
        this.terminal = terminal;
        this.pagerSupport = new PagerSupport(terminal, configManagerProvider);
    }

    private void createTailTipWidgets(SystemRegistryImpl registry, LineReader reader) {
        tailTipWidgets = new TailTipWidgets(reader, registry::commandDescription, TAIL_TIP_DESCRIPTION_LINES,
                TailTipWidgets.TipType.COMPLETER);
        tailTipWidgets.enable();
        // Workaround for the scroll truncation issue in windows terminal
        // Turn off tailtip widgets before printing to the output
        CommandLineContextProvider.setPrintWrapper(printer -> {
            tailTipWidgets.disable();
            printer.run();
            tailTipWidgets.enable();
        });
        // Workaround for jline issue where TailTipWidgets will produce NPE when passed a bracket
        registry.setScriptDescription(cmdLine -> null);
    }

    /**
     * Displays output, using pager if needed. Disables TailTipWidgets during output
     * to prevent visual glitches.
     */
    private void outputWithPager(String output) {
        if (tailTipWidgets != null) {
            tailTipWidgets.disable();
        }

        pagerSupport.write(output);

        if (tailTipWidgets != null) {
            tailTipWidgets.enable();
        }
    }

    /**
     * Executor method. This is thread blocking method, until REPL stop executing.
     *
     * @param repl data class of executing REPL.
     */
    @Override
    public void execute(Repl repl) {
        // Save the previous context to restore it when this REPL exits.
        // This is important for nested REPLs (e.g., SQL REPL inside main REPL).
        CommandLineContext previousContext = CommandLineContextProvider.getContext();
        try {
            repl.customizeTerminal(terminal);

            IgnitePicocliCommands picocliCommands = createPicocliCommands(repl);
            SystemRegistryImpl registry = createRegistry(repl);
            registry.setCommandRegistries(picocliCommands);

            LineReader reader = createReader(repl, registry);
            RegistryCommandExecutor executor = new RegistryCommandExecutor(parser, picocliCommands.getCmd());

            setupWidgets(repl, registry, reader);
            repl.onStart();

            runReplLoop(repl, picocliCommands, executor, reader);

            reader.getHistory().save();
        } catch (Throwable t) {
            exceptionHandlers.handleException(System.err::println, t);
        } finally {
            // Restore the previous context so the parent REPL continues to work correctly
            CommandLineContextProvider.setContext(previousContext);
        }
    }

    private SystemRegistryImpl createRegistry(Repl repl) {
        return new SystemRegistryImpl(
                repl.getParser() != null ? repl.getParser() : parser,
                terminal,
                WORK_DIR_PROVIDER,
                null
        );
    }

    private LineReader createReader(Repl repl, SystemRegistryImpl registry) {
        LineReader reader = createReader(
                repl.getCompleter() != null ? repl.getCompleter() : registry.completer(),
                repl.getHighlighter() != null ? repl.getHighlighter() : new DefaultHighlighter(),
                repl.getParser() != null ? repl.getParser() : parser
        );
        if (repl.getHistoryFileName() != null) {
            reader.variable(LineReader.HISTORY_FILE, StateFolderProvider.getStateFile(repl.getHistoryFileName()));
        }
        return reader;
    }

    private LineReader createReader(Completer completer, Highlighter highlighter, Parser parser) {
        LineReader result = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(completer)
                .highlighter(highlighter)
                .parser(parser)
                .expander(new NoopExpander())
                .variable(LineReader.LIST_MAX, MAX_TAB_COMPLETION_CANDIDATES)
                .build();
        result.setAutosuggestion(SuggestionType.COMPLETER);
        return result;
    }

    /**
     * Main REPL loop. Reads commands, executes them, and displays output with pager support.
     *
     * <p>Output capture flow:
     * <ol>
     *   <li>Command output is redirected to a StringWriter instead of the terminal</li>
     *   <li>After the command completes, we have the full output as a string</li>
     *   <li>PagerSupport decides: if output exceeds terminal height, pipe through pager (e.g., less);
     *       otherwise, print directly to terminal</li>
     * </ol>
     *
     * <p>This approach allows us to measure output size before displaying it.
     */
    private void runReplLoop(Repl repl, IgnitePicocliCommands picocliCommands, RegistryCommandExecutor executor, LineReader reader) {
        while (!interrupted.get()) {
            try {
                executor.cleanUp();
                String line = reader.readLine(repl.getPromptProvider().getPrompt(), null, (MaskingCallback) null, null);
                if (line.isEmpty()) {
                    continue;
                }

                String output = executeCommandWithOutputCapture(repl, picocliCommands, executor, line);
                if (!output.isEmpty()) {
                    outputWithPager(output);
                }
            } catch (Throwable t) {
                exceptionHandlers.handleException(System.err::println, t);
            }
        }
    }

    /**
     * Executes a command and captures its output to a string.
     *
     * <p>Temporarily redirects picocli's output writer to a StringWriter,
     * executes the command pipeline, then restores the original terminal writer.
     */
    private String executeCommandWithOutputCapture(Repl repl, IgnitePicocliCommands picocliCommands,
            RegistryCommandExecutor executor, String line) {
        StringWriter capturedOutput = new StringWriter();
        PrintWriter capturedWriter = new PrintWriter(capturedOutput);

        picocliCommands.getCmd().setOut(capturedWriter);
        try {
            repl.getPipeline(executor, exceptionHandlers, line).runPipeline();
        } finally {
            picocliCommands.getCmd().setOut(terminal.writer());
        }

        capturedWriter.flush();
        return capturedOutput.toString();
    }

    private void setupWidgets(Repl repl, SystemRegistryImpl registry, LineReader reader) {
        if (repl.isTailTipWidgetsEnabled()) {
            createTailTipWidgets(registry, reader);
        } else if (repl.isAutosuggestionsWidgetsEnabled()) {
            AutosuggestionWidgets widgets = new AutosuggestionWidgets(reader);
            widgets.enable();
        }
    }

    private IgnitePicocliCommands createPicocliCommands(Repl repl) throws Exception {
        CommandLine cmd = new CommandLine(repl.commandClass(), factory);
        IDefaultValueProvider defaultValueProvider = repl.defaultValueProvider();
        if (defaultValueProvider != null) {
            cmd.setDefaultValueProvider(defaultValueProvider);
        }
        CommandLineContextProvider.setCmd(cmd);
        cmd.setExecutionExceptionHandler(new PicocliExecutionExceptionHandler(exceptionHandlers));
        cmd.setTrimQuotes(true);
        cmd.setCaseInsensitiveEnumValuesAllowed(true);

        DynamicCompleterRegistry completerRegistry = factory.create(DynamicCompleterRegistry.class);
        DynamicCompleterActivationPoint activationPoint = factory.create(DynamicCompleterActivationPoint.class);
        activationPoint.activateDynamicCompleter(completerRegistry);

        DynamicCompleterFilter dynamicCompleterFilter = factory.create(DynamicCompleterFilter.class);
        List<CompleterFilter> filters = List.of(dynamicCompleterFilter,
                new ShortOptionsFilter(),
                new NonRepeatableOptionsFilter(cmd.getCommandSpec()),
                new DeployUnitsOptionsFilter()
        );

        return new IgnitePicocliCommands(cmd, completerRegistry, filters);
    }
}
