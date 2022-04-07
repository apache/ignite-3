package org.apache.ignite.cli.core.repl.executor;

import org.apache.ignite.cli.commands.decorators.core.CommandOutput;

public interface CommandExecutor {
    CommandOutput execute(String line) throws Exception;
    void cleanUp();
    void trace(Exception e);
}
