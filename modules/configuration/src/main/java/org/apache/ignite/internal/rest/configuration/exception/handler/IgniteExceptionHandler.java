package org.apache.ignite.internal.rest.configuration.exception.handler;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.rest.api.ErrorResult;
import org.apache.ignite.lang.IgniteException;

/**
 * Handles {@link ConfigurationValidationException} and represents it as a rest response.
 */
@Produces
@Singleton
@Requires(classes = {IgniteException.class, ExceptionHandler.class})
public class IgniteExceptionHandler implements ExceptionHandler<IgniteException, HttpResponse<ErrorResult>> {

    @Override
    public HttpResponse<ErrorResult> handle(HttpRequest request, IgniteException exception) {
        final ErrorResult errorResult = new ErrorResult("APPLICATION_EXCEPTION", exception.getMessage());
        return HttpResponse.badRequest().body(errorResult);
    }
}
