package org.apache.ignite.cli.core.exception.handler;

import org.apache.ignite.cli.core.exception.ConnectCommandException;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;

/**
 * Exception handler for {@link ConnectCommandException}.
 */
public class ConnectCommandExceptionHandler implements ExceptionHandler<ConnectCommandException> {
    @Override
    public void handle(ExceptionWriter err, ConnectCommandException e) {
        err.write(e.getReason());
    }

    @Override
    public Class<ConnectCommandException> applicableException() {
        return ConnectCommandException.class;
    }
}
