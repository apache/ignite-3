package org.apache.ignite.internal.cluster.management.rest.exception.handler;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpResponseFactory;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.rest.api.ErrorResult;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Handles {@link IgniteInternalException} and represents it as a rest response.
 */
@Produces
@Singleton
@Requires(classes = {IgniteInternalException.class, ExceptionHandler.class})
public class IgniteInternalExceptionHandler implements ExceptionHandler<IgniteInternalException, HttpResponse<ErrorResult>> {

    @Override
    public HttpResponse<ErrorResult> handle(HttpRequest request, IgniteInternalException exception) {
        final ErrorResult errorResult = new ErrorResult("ALREADY_INITIALIZED", exception.getMessage());
        return HttpResponseFactory.INSTANCE.status(HttpStatus.CONFLICT).body(errorResult);
    }
}
