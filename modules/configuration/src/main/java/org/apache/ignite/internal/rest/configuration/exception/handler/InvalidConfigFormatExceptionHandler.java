package org.apache.ignite.internal.rest.configuration.exception.handler;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.rest.api.ErrorResult;
import org.apache.ignite.internal.rest.configuration.exception.InvalidConfigFormatException;

/**
 * Handles {@link InvalidConfigFormatException} and represents it as a rest response.
 */
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@Requires(classes = {InvalidConfigFormatException.class, ExceptionHandler.class})
public class InvalidConfigFormatExceptionHandler implements ExceptionHandler<InvalidConfigFormatException, HttpResponse<ErrorResult>> {

    @Override
    public HttpResponse<ErrorResult> handle(HttpRequest request, InvalidConfigFormatException exception) {
        final ErrorResult errorResult = new ErrorResult("INVALID_CONFIG_FORMAT", exception.getMessage());
        return HttpResponse.badRequest().body(errorResult);
    }
}
