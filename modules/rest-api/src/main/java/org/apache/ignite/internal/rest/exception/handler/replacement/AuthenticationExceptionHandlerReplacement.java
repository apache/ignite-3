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
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationExceptionHandler;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.HttpCode;
import org.apache.ignite.internal.rest.problem.HttpProblemResponse;

/**
 * Replacement for {@link AuthenticationExceptionHandler}. Returns {@link HttpProblemResponse}.
 */
@Singleton
@Replaces(AuthenticationExceptionHandler.class)
@Requires(classes = {AuthenticationException.class, ExceptionHandler.class})
public class AuthenticationExceptionHandlerReplacement implements
        ExceptionHandler<AuthenticationException, HttpResponse<? extends Problem>> {
    @Override
    public HttpResponse<? extends Problem> handle(HttpRequest request, AuthenticationException exception) {
        return HttpProblemResponse.from(
                Problem.fromHttpCode(HttpCode.UNAUTHORIZED)
                        .detail(exception.getMessage())
        );
    }
}
