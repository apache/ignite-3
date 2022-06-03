package org.apache.ignite.cli.core.exception.handler;

import org.apache.ignite.cli.core.exception.ExceptionHandlers;

/**
 * Default collection of exception handlers.
 */
public class DefaultExceptionHandlers extends ExceptionHandlers {

    /**
     * Constructor.
     */
    public DefaultExceptionHandlers() {
        addExceptionHandler(new SqlExceptionHandler());
        addExceptionHandler(new ConnectCommandExceptionHandler());
        addExceptionHandler(new CommandExecutionExceptionHandler());
        addExceptionHandler(new TimeoutExceptionHandler());
        addExceptionHandler(new IgniteClientExceptionHandler());
        addExceptionHandler(new IgniteCliExceptionHandler());
        addExceptionHandler(new ConnectExceptionHandler());
        addExceptionHandler(new ApiExceptionHandler());
        addExceptionHandler(new UnknownCommandExceptionHandler());
    }
}
