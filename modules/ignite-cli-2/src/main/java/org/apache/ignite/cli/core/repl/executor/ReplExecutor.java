package org.apache.ignite.cli.core.repl.executor;

import java.util.Map.Entry;
import org.apache.ignite.cli.core.repl.Repl;
import org.fusesource.jansi.AnsiConsole;
import org.jline.console.impl.Builtins;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

public class ReplExecutor {
    private final PicocliCommandsFactory factory;
    private final Terminal terminal;

    public ReplExecutor(PicocliCommandsFactory factory, Terminal terminal) {
        this.factory = factory;
        this.terminal = terminal;
        factory.setTerminal(terminal);
    }

    public void execute(Repl repl) {
        AnsiConsole.systemInstall();
        try {
            repl.customizeTerminal(terminal);

            Builtins builtins = createBuiltins(repl);
            PicocliCommands picocliCommands = createPicocliCommands(repl);
            SystemRegistryImpl registry = createRegistry(repl);
            registry.setCommandRegistries(builtins, picocliCommands);

            LineReader reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .completer(registry.completer())
                    .parser(repl.getParser())
                    .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                    .build();
            builtins.setLineReader(reader);
            CommandExecutor commandExecutor = repl.commandExecutorProvider().get(factory, registry, picocliCommands, reader);

            // start the shell and process input until the user quits with Ctrl-D
            String line;
            String name = repl.getName() + "> ";
            while (true) {
                try {
                    commandExecutor.cleanUp();
                    line = reader.readLine(name, null, (MaskingCallback) null, null);
                    //TODO: Add decorators support
                    System.out.println(commandExecutor.execute(line));
                } catch (UserInterruptException e) {
                    // Ignore
                } catch (EndOfFileException e) {
                    return;
                } catch (Exception e) {
                    commandExecutor.trace(e);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            AnsiConsole.systemUninstall();
        }
    }

    private Builtins createBuiltins(Repl repl) {
        // set up JLine built-in commands
        Builtins builtins = new Builtins(repl.getWorkDirProvider(), null, null);
        for (Entry<String, String> aliases : repl.getAliases().entrySet()) {
            builtins.alias(aliases.getKey(), aliases.getValue());
        }
        return builtins;
    }

    private SystemRegistryImpl createRegistry(Repl repl) {
        return new SystemRegistryImpl(repl.getParser(), terminal, repl.getWorkDirProvider(), null);
    }

    private PicocliCommands createPicocliCommands(Repl repl) {
        CommandLine cmd = new CommandLine(repl.getCommand(), factory);
        return new PicocliCommands(cmd);
    }
}
