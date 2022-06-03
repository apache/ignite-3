package org.apache.ignite.cli.core.exception.handler;

import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.jline.console.impl.SystemRegistryImpl.UnknownCommandException;

/**
 * Exception handler for {@link UnknownCommandException}.
 * This exception is thrown by {@link org.jline.console.SystemRegistry#execute(String)} when the user types invalid or unknown command.
 */
public class UnknownCommandExceptionHandler implements ExceptionHandler<UnknownCommandException> {

    @Override
    public void handle(ExceptionWriter err, UnknownCommandException e) {
        err.write(e.getMessage());
    }

    @Override
    public Class<UnknownCommandException> applicableException() {
        return UnknownCommandException.class;
    }
}
