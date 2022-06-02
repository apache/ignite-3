package org.apache.ignite.cli.core.exception.handler;

import org.apache.ignite.cli.core.exception.CommandExecutionException;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception handler for {@link CommandExecutionException}.
 */
public class CommandExecutionExceptionHandler implements ExceptionHandler<CommandExecutionException> {
    private static final Logger log = LoggerFactory.getLogger(CommandExecutionExceptionHandler.class);

    @Override
    public void handle(ExceptionWriter err, CommandExecutionException e) {
        log.error("Command {} failed with reason {}", e.getCommandId(), e.getReason(), e);
        err.write(String.format("Command %s failed with reason: %s", e.getCommandId(), e.getReason()));
    }

    @Override
    public Class<CommandExecutionException> applicableException() {
        return CommandExecutionException.class;
    }
}
