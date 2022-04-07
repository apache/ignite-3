package org.apache.ignite.cli.core.repl.executor;


import org.apache.ignite.cli.commands.decorators.core.CommandOutput;
import org.jline.console.SystemRegistry;
import org.jline.reader.LineReader;
import org.jline.widget.TailTipWidgets;
import picocli.CommandLine.IFactory;
import picocli.shell.jline3.PicocliCommands;

public class RegistryCommandExecutor implements CommandExecutor {
    private final SystemRegistry systemRegistry;

    public RegistryCommandExecutor(IFactory factory, SystemRegistry systemRegistry, PicocliCommands picocliCommands, LineReader reader) {
        this.systemRegistry = systemRegistry;
        systemRegistry.register("help", picocliCommands);

        TailTipWidgets widgets = new TailTipWidgets(reader, systemRegistry::commandDescription, 5,
                TailTipWidgets.TipType.COMPLETER);
        widgets.enable();
    }

    @Override
    public CommandOutput execute(String line) throws Exception {
        Object execute = systemRegistry.execute(line);
        return execute::toString;
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
