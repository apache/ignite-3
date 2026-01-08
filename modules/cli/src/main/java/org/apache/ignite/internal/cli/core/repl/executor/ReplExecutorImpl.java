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
import org.apache.ignite.internal.cli.core.repl.terminal.PagerSupport;
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
import org.apache.ignite.internal.cli.core.repl.context.CommandLineContextProvider;
import org.apache.ignite.internal.cli.core.repl.expander.NoopExpander;
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

    private Parser parser = new DefaultParser().escapeChars(null);

    private final Supplier<Path> workDirProvider = () -> Paths.get(System.getProperty("user.dir"));

    private final AtomicBoolean interrupted = new AtomicBoolean();

    private final ExceptionHandlers exceptionHandlers = new ReplExceptionHandlers(interrupted::set);

    private final PicocliCommandsFactory factory;

    private final Terminal terminal;

    private final PagerSupport pagerSupport;

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

    private TailTipWidgets tailTipWidgets;

    private void createTailTipWidgets(SystemRegistryImpl registry, LineReader reader) {
        tailTipWidgets = new TailTipWidgets(reader, registry::commandDescription, 5,
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

    private void outputWithPager(String output) {
        if (tailTipWidgets != null) {
            tailTipWidgets.disable();
        }

        if (pagerSupport.shouldUsePager(output)) {
            pagerSupport.pipeToPage(output);
        } else {
            terminal.writer().print(output);
            terminal.writer().flush();
        }

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
        try {
            repl.customizeTerminal(terminal);

            IgnitePicocliCommands picocliCommands = createPicocliCommands(repl);
            SystemRegistryImpl registry = new SystemRegistryImpl(
                    repl.getParser() == null
                            ? parser
                            : repl.getParser(),
                    terminal,
                    workDirProvider,
                    null
            );

            registry.setCommandRegistries(picocliCommands);

            LineReader reader = createReader(
                    repl.getCompleter() != null
                            ? repl.getCompleter()
                            : registry.completer(),
                    repl.getHighlighter() != null
                            ? repl.getHighlighter()
                            : new DefaultHighlighter(),
                    repl.getParser() != null
                            ? repl.getParser()
                            : parser
            );
            if (repl.getHistoryFileName() != null) {
                reader.variable(LineReader.HISTORY_FILE, StateFolderProvider.getStateFile(repl.getHistoryFileName()));
            }

            RegistryCommandExecutor executor = new RegistryCommandExecutor(parser, picocliCommands.getCmd());

            setupWidgets(repl, registry, reader);

            repl.onStart();

            while (!interrupted.get()) {
                try {
                    executor.cleanUp();
                    String prompt = repl.getPromptProvider().getPrompt();
                    String line = reader.readLine(prompt, null, (MaskingCallback) null, null);
                    if (line.isEmpty()) {
                        continue;
                    }

                    // Capture output during command execution for pager support
                    StringWriter capturedOutput = new StringWriter();
                    PrintWriter capturedWriter = new PrintWriter(capturedOutput);
                    picocliCommands.getCmd().setOut(capturedWriter);

                    repl.getPipeline(executor, exceptionHandlers, line).runPipeline();

                    // Output with pager if needed
                    capturedWriter.flush();
                    String output = capturedOutput.toString();
                    if (!output.isEmpty()) {
                        outputWithPager(output);
                    }

                    // Restore original writer
                    picocliCommands.getCmd().setOut(terminal.writer());
                } catch (Throwable t) {
                    exceptionHandlers.handleException(System.err::println, t);
                }
            }
            reader.getHistory().save();
        } catch (Throwable t) {
            exceptionHandlers.handleException(System.err::println, t);
        }
    }

    private void setupWidgets(Repl repl, SystemRegistryImpl registry, LineReader reader) {
        if (repl.isTailTipWidgetsEnabled()) {
            createTailTipWidgets(registry, reader);
        } else if (repl.isAutosuggestionsWidgetsEnabled()) {
            AutosuggestionWidgets widgets = new AutosuggestionWidgets(reader);
            widgets.enable();
        }
    }

    private LineReader createReader(Completer completer, Highlighter highlighter, Parser parser) {
        LineReader result = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(completer)
                .highlighter(highlighter)
                .parser(parser)
                .expander(new NoopExpander())
                .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                .build();
        result.setAutosuggestion(SuggestionType.COMPLETER);
        return result;
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
