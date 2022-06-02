package org.apache.ignite.cli.core.exception.handler;

import java.net.ConnectException;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;

/**
 * Exception handler for {@link ConnectException}.
 */
public class ConnectExceptionHandler implements ExceptionHandler<ConnectException> {
    @Override
    public void handle(ExceptionWriter err, ConnectException e) {
        err.write("Connection failed " + e.getMessage());
    }

    @Override
    public Class<ConnectException> applicableException() {
        return ConnectException.class;
    }
}
