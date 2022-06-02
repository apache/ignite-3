package org.apache.ignite.cli.core.exception.handler;

import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Exception handler for {@link ApiException}.
 */
public class ApiExceptionHandler implements ExceptionHandler<ApiException> {
    @Override
    public void handle(ExceptionWriter err, ApiException e) {
        err.write("Api error: " + e.getResponseBody());
    }

    @Override
    public Class<ApiException> applicableException() {
        return ApiException.class;
    }
}
