package org.apache.ignite.cli.core.repl.executor;

import org.jline.console.impl.SystemRegistryImpl;
import org.jline.reader.LineReader;
import picocli.CommandLine.IFactory;
import picocli.shell.jline3.PicocliCommands;

public interface CommandExecutorProvider {
    CommandExecutor get(IFactory factory, SystemRegistryImpl systemRegistry, PicocliCommands picocliCommands, LineReader reader);
}
