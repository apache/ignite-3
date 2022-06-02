package org.apache.ignite.cli.core.exception.handler;

import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.deprecated.IgniteCliException;

/**
 * Exception handler for {@link IgniteCliException}.
 */
public class IgniteCliExceptionHandler implements ExceptionHandler<IgniteCliException> {
    @Override
    public void handle(ExceptionWriter err, IgniteCliException e) {
        err.write(e.getMessage());
    }

    @Override
    public Class<IgniteCliException> applicableException() {
        return IgniteCliException.class;
    }
}
