package org.apache.ignite.cli.core.repl;

import java.nio.file.Path;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.cli.core.repl.executor.CommandExecutorProvider;
import org.apache.ignite.cli.core.repl.terminal.TerminalCustomizer;
import org.jline.reader.Completer;
import org.jline.reader.Parser;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;

/**
 * Data class with all information about REPL.
 */
public class Repl {
    private final String name;
    private final Object command;
    private final Supplier<Path> workDirProvider;
    private final Map<String, String> aliases;
    private final Parser parser = new DefaultParser();
    private final Completer completer;
    private final CommandExecutorProvider commandExecutorProvider;
    private final TerminalCustomizer terminalCustomizer;
    private final Runnable onSleep;
    private final Runnable onWakeUp;
    private final Runnable onEnable;
    private final Runnable onDispose;
    
    public Repl(String name,
            Object command,
            Supplier<Path> workDirProvider,
            Map<String, String> aliases,
            Completer completer,
            CommandExecutorProvider commandExecutorProvider,
            TerminalCustomizer terminalCustomizer, Runnable onSleep, Runnable onWakeUp, Runnable onEnable, Runnable onDispose) {
        this.name = name;
        this.command = command;
        this.workDirProvider = workDirProvider;
        this.aliases = aliases;
        this.completer = completer;
        this.commandExecutorProvider = commandExecutorProvider;
        this.terminalCustomizer = terminalCustomizer;
        this.onSleep = onSleep;
        this.onWakeUp = onWakeUp;
        this.onEnable = onEnable;
        this.onDispose = onDispose;
    }
    
    public static ReplBuilder builder() {
        return new ReplBuilder();
    }
    
    public String getName() {
        return name;
    }
    
    public Supplier<Path> getWorkDirProvider() {
        return workDirProvider;
    }
    
    public Map<String, String> getAliases() {
        return aliases;
    }
    
    public Object getCommand() {
        return command;
    }
    
    public CommandExecutorProvider commandExecutorProvider() {
        return commandExecutorProvider;
    }
    
    public void customizeTerminal(Terminal terminal) {
        terminalCustomizer.customize(terminal);
    }
    
    public Parser getParser() {
        return parser;
    }
    
    public Completer completer() {
        return completer;
    }
    
    public void onSleep() {
        safeRun(onSleep);
    }
    
    public void onWakeUp() {
        safeRun(onWakeUp);
    }
    
    public void onEnable() {
        safeRun(onEnable);
    }

    public void dispose() {
        safeRun(onDispose);
    }

    private static void safeRun(Runnable runnable) {
        if (runnable != null) {
            runnable.run();
        }
    }
}
