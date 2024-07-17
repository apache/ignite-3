/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.rest.exception.handler.replacement;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import io.micronaut.validation.exceptions.ConstraintExceptionHandler;
import jakarta.inject.Singleton;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.ElementKind;
import javax.validation.Path;
import javax.validation.Path.Node;
import org.apache.ignite.internal.rest.api.InvalidParam;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.HttpCode;
import org.apache.ignite.internal.rest.problem.HttpProblemResponse;

/**
 * Replacement for {@link ConstraintExceptionHandler}. Returns {@link HttpProblemResponse}.
 */
@Singleton
@Replaces(ConstraintExceptionHandler.class)
@Requires(classes = {ConstraintViolationException.class, ExceptionHandler.class})
public class ConstraintExceptionHandlerReplacement implements ExceptionHandler<ConstraintViolationException, HttpResponse<?>> {
    @Override
    public HttpResponse<? extends Problem> handle(HttpRequest request, ConstraintViolationException exception) {
        Set<InvalidParam> invalidParams = exception.getConstraintViolations()
                .stream()
                .map(it -> new InvalidParam(it.getPropertyPath().toString(), buildMessage(it)))
                .collect(Collectors.toSet());

        return HttpProblemResponse.from(
                Problem.fromHttpCode(HttpCode.BAD_REQUEST)
                        .detail("Validation failed")
                        .invalidParams(invalidParams)
        );
    }

    private static String buildMessage(ConstraintViolation<?> violation) {
        Path propertyPath = violation.getPropertyPath();
        StringBuilder message = new StringBuilder();
        Iterator<Node> i = propertyPath.iterator();

        while (i.hasNext()) {
            Path.Node node = i.next();

            if (node.getKind() == ElementKind.METHOD || node.getKind() == ElementKind.CONSTRUCTOR) {
                continue;
            }

            message.append(node.getName());

            if (node.getIndex() != null) {
                message.append(String.format("[%d]", node.getIndex()));
            }

            if (i.hasNext()) {
                message.append('.');
            }
        }

        message.append(": ").append(violation.getMessage());

        return message.toString();
    }
}
