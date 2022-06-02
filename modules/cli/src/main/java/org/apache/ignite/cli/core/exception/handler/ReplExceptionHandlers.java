package org.apache.ignite.cli.core.exception.handler;

import java.util.function.Consumer;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;

/**
 * Collection of exception handlers for REPL.
 */
public class ReplExceptionHandlers extends ExceptionHandlers {

    /**
     * Constructor.
     *
     * @param stop REPL stop action.
     */
    public ReplExceptionHandlers(Consumer<Boolean> stop) {
        addExceptionHandler(new EndOfFileExceptionHandler(stop));
        addExceptionHandler(new UserInterruptExceptionHandler());
    }
}
