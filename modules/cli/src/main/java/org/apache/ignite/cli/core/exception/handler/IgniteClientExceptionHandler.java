package org.apache.ignite.cli.core.exception.handler;

import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.client.IgniteClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception handler for {@link IgniteClientException}.
 */
public class IgniteClientExceptionHandler implements ExceptionHandler<IgniteClientException> {
    private static final Logger log = LoggerFactory.getLogger(IgniteClientExceptionHandler.class);

    @Override
    public void handle(ExceptionWriter err, IgniteClientException e) {
        log.error("Ignite client exception", e);
        err.write("Ignite client exception with code: " + e.errorCode());
    }

    @Override
    public Class<IgniteClientException> applicableException() {
        return IgniteClientException.class;
    }
}
