package org.apache.ignite.cli.core.exception.handler;

import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.jline.reader.UserInterruptException;

/**
 * Exception handler for {@link UserInterruptException}.
 */
public class UserInterruptExceptionHandler implements ExceptionHandler<UserInterruptException> {
    @Override
    public void handle(ExceptionWriter err, UserInterruptException e) {
        //NOOP
    }

    @Override
    public Class<UserInterruptException> applicableException() {
        return UserInterruptException.class;
    }
}
