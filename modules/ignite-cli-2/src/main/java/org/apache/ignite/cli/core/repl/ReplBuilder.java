package org.apache.ignite.cli.core.repl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.cli.commands.decorators.core.CommandOutput;
import org.apache.ignite.cli.core.repl.action.CustomAction;
import org.apache.ignite.cli.core.repl.action.CustomAction.ReplFunc;
import org.apache.ignite.cli.core.repl.executor.CommandExecutorProvider;
import org.apache.ignite.cli.core.repl.terminal.TerminalCustomizer;
import org.jline.reader.Completer;

public class ReplBuilder {
    private String name = "default_cli_name";
    private Supplier<Path> workDirProvider = () -> Paths.get(System.getProperty("user.dir"));
    private final Map<String, String> aliases = new HashMap<>();
    private Object command;
    private Completer completer;
    private CommandExecutorProvider commandExecutorProvider;
    private TerminalCustomizer terminalCustomizer = terminal -> {};
    private Runnable onSleep;
    private Runnable onWakeUp;
    private Runnable onEnable;
    private Runnable onDispose;
    
    public Repl build() {
        return new Repl(name,
                command,
                workDirProvider,
                aliases,
                completer,
                commandExecutorProvider,
                terminalCustomizer,
                onSleep,
                onWakeUp,
                onEnable,
                onDispose);
    }
    
    public ReplBuilder withName(String name) {
        this.name = name;
        return this;
    }
    
    public ReplBuilder withCommandsClass(Class<?> commandsClass) {
        this.command = commandsClass;
        return this;
    }
    
    public ReplBuilder withCustomAction(ReplFunc<String, ? extends CommandOutput> customAction) {
        this.command = new CustomAction(customAction);
        return this;
    }
    
    public ReplBuilder withCommandExecutorProvider(CommandExecutorProvider commandExecutorProvider) {
        this.commandExecutorProvider = commandExecutorProvider;
        return this;
    }
    
    public ReplBuilder withWorkDirProvider(Supplier<Path> workDirProvider) {
        this.workDirProvider = workDirProvider;
        return this;
    }
    
    public ReplBuilder withAliases(Map<String, String> aliases) {
        this.aliases.putAll(aliases);
        return this;
    }
    
    public ReplBuilder withTerminalCustomizer(TerminalCustomizer terminalCustomizer) {
        this.terminalCustomizer = terminalCustomizer;
        return this;
    }
    
    public ReplBuilder withCompleter(Completer completer) {
        this.completer = completer;
        return this;
    }
    
    public ReplBuilder withOnSleep(Runnable onSleep) {
        this.onSleep = onSleep;
        return this;
    }
    
    public ReplBuilder withOnWakeUp(Runnable onWakeUp) {
        this.onWakeUp = onWakeUp;
        return this;
    }
    
    public ReplBuilder withOnEnable(Runnable onEnable) {
        this.onEnable = onEnable;
        return this;
    }
    
    public ReplBuilder withOnDispose(Runnable onDispose) {
        this.onDispose = onDispose;
        return this;
    }
}
