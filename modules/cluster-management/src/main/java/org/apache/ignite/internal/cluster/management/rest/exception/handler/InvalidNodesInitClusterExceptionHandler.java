package org.apache.ignite.internal.cluster.management.rest.exception.handler;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cluster.management.rest.exception.InvalidNodesInitClusterException;
import org.apache.ignite.internal.rest.api.ErrorResult;

/**
 * Handles {@link InvalidNodesInitClusterException} and represents it as a rest response.
 */
@Produces
@Singleton
@Requires(classes = {InvalidNodesInitClusterException.class, ExceptionHandler.class})
public class InvalidNodesInitClusterExceptionHandler implements
        ExceptionHandler<InvalidNodesInitClusterException, HttpResponse<ErrorResult>> {

    @Override
    public HttpResponse<ErrorResult> handle(HttpRequest request, InvalidNodesInitClusterException exception) {
        final ErrorResult errorResult = new ErrorResult("INVALID_NODES", exception.getMessage());
        return HttpResponse.badRequest().body(errorResult);
    }
}
