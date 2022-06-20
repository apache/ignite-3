/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.rest.configuration.exception.handler;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.rest.api.InvalidParam;
import org.apache.ignite.internal.rest.api.RestApiMediaType;
import org.apache.ignite.internal.rest.api.ValidationProblem;

/**
 * Handles {@link ConfigurationValidationException} and represents it as a rest response.
 */
@Produces(RestApiMediaType.APPLICATION_JSON)
@Singleton
@Requires(classes = {ConfigurationValidationException.class, ExceptionHandler.class})
public class ConfigurationValidationExceptionHandler implements
        ExceptionHandler<ConfigurationValidationException, HttpResponse<ValidationProblem>> {

    @Override
    // todo: propagate invalid parameter name in ValidationIssue
    public HttpResponse<ValidationProblem> handle(HttpRequest request, ConfigurationValidationException exception) {
        List<InvalidParam> invalidParams = exception.getIssues()
                .stream()
                .map(ValidationIssue::message)
                .map(message -> new InvalidParam("changeme", message))
                .collect(Collectors.toList());

        ValidationProblem problem = ValidationProblem.validationProblemBuilder()
                .status(400)
                .detail("Parameters validation did not pass")
                .invalidParams(invalidParams)
                .build();

        return HttpResponse.badRequest().body(problem);
    }
}
