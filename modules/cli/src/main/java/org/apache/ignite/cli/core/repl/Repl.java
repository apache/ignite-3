package org.apache.ignite.cli.core.repl;

import java.util.Map;
import org.apache.ignite.cli.core.CallExecutionPipelineProvider;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.cli.core.repl.executor.RegistryCommandExecutor;
import org.apache.ignite.cli.core.repl.prompt.PromptProvider;
import org.apache.ignite.cli.core.repl.terminal.TerminalCustomizer;
import org.jline.reader.Completer;
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
     */
    public Repl(PromptProvider promptProvider,
            Class<?> commandClass,
            IDefaultValueProvider defaultValueProvider,
            Map<String, String> aliases,
            TerminalCustomizer terminalCustomizer,
            CallExecutionPipelineProvider provider,
            Completer completer,
            String historyFileName,
            boolean tailTipWidgetsEnabled
    ) {
        this.promptProvider = promptProvider;
        this.commandClass = commandClass;
        this.defaultValueProvider = defaultValueProvider;
        this.aliases = aliases;
        this.terminalCustomizer = terminalCustomizer;
        this.provider = provider;
        this.completer = completer;
        this.historyFileName = historyFileName;
        this.tailTipWidgetsEnabled = tailTipWidgetsEnabled;
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
}
