package org.apache.ignite.cli.core.exception.handler;

import java.util.concurrent.TimeoutException;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception handler for {@link TimeoutException}.
 */
public class TimeoutExceptionHandler implements ExceptionHandler<TimeoutException> {
    private static final Logger log = LoggerFactory.getLogger(TimeoutExceptionHandler.class);

    @Override
    public void handle(ExceptionWriter err, TimeoutException e) {
        log.error("Timeout exception ", e);
        err.write("Command failed with timeout.");
    }

    @Override
    public Class<TimeoutException> applicableException() {
        return TimeoutException.class;
    }
}
