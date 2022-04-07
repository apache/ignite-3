package org.apache.ignite.cli.core.repl.executor;

import java.util.Objects;
import org.apache.ignite.cli.commands.decorators.core.CommandOutput;
import org.jline.console.SystemRegistry;
import org.jline.reader.LineReader;
import org.jline.widget.TailTipWidgets;
import picocli.CommandLine;
import picocli.CommandLine.IFactory;
import picocli.shell.jline3.PicocliCommands;

public class SqlReplCommandExecutor implements CommandExecutor {

    private final CommandLine clearCommandLine;
    private final SystemRegistry systemRegistry;

    public SqlReplCommandExecutor(IFactory factory, SystemRegistry systemRegistry, PicocliCommands picocliCommands, LineReader reader) {
        this.systemRegistry = systemRegistry;
        systemRegistry.register("help", picocliCommands);

        clearCommandLine = new CommandLine(PicocliCommands.ClearScreen.class, factory);

        TailTipWidgets widgets = new TailTipWidgets(reader, systemRegistry::commandDescription, 5,
            TailTipWidgets.TipType.COMPLETER);
        widgets.enable();
    }
    
    @Override
    public CommandOutput execute(String line) throws Exception {
        //TODO: temporary hint to support clear command
        if (Objects.equals(line, "clear")) {
            clearCommandLine.execute(line);
            return null;
        }
        Object execute = systemRegistry.execute(line);
        return execute == null ? null : execute::toString;
    }
    
    @Override
    public void cleanUp() {
        systemRegistry.cleanUp();
    }
    
    @Override
    public void trace(Exception e) {
        systemRegistry.trace(e);
    }
}
