package org.apache.ignite.internal.rest.configuration.exception.handler;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.rest.api.ErrorResult;
import org.apache.ignite.internal.rest.configuration.exception.ConfigPathUnrecognizedException;

/**
 * Handles {@link ConfigPathUnrecognizedException} and represents it as a rest response.
 */
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@Requires(classes = {ConfigPathUnrecognizedException.class, ExceptionHandler.class})
public class ConfigPathUnrecognizedExceptionHandler implements
        ExceptionHandler<ConfigPathUnrecognizedException, HttpResponse<ErrorResult>> {

    @Override
    public HttpResponse<ErrorResult> handle(HttpRequest request, ConfigPathUnrecognizedException exception) {
        final ErrorResult errorResult = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", exception.getMessage());
        return HttpResponse.badRequest().body(errorResult);
    }
}
