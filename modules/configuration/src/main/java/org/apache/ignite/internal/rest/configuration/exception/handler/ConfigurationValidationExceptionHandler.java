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
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.rest.api.ErrorResult;

/**
 * Handles {@link ConfigurationValidationException} and represents it as a rest response.
 */
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@Requires(classes = {ConfigurationValidationException.class, ExceptionHandler.class})
public class ConfigurationValidationExceptionHandler implements
        ExceptionHandler<ConfigurationValidationException, HttpResponse<ErrorResult>> {

    @Override
    public HttpResponse<ErrorResult> handle(HttpRequest request, ConfigurationValidationException exception) {
        String joinedMessage = exception.getIssues()
                .stream()
                .map(ValidationIssue::message)
                .collect(Collectors.joining(","));
        final ErrorResult errorResult = new ErrorResult("VALIDATION_EXCEPTION", joinedMessage);
        return HttpResponse.badRequest().body(errorResult);
    }
}
