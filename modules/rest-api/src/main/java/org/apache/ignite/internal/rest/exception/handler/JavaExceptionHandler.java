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

package org.apache.ignite.internal.rest.exception.handler;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.HttpCode;
import org.apache.ignite.internal.rest.problem.HttpProblemResponse;

/**
 * Handles {@link Exception} and represents it as an application/problem+json response. This will catch all unhandled exceptions since all
 * REST endpoints are marked as producing problem json.
 */
@Singleton
@Requires(classes = {Exception.class, ExceptionHandler.class})
public class JavaExceptionHandler implements ExceptionHandler<Exception, HttpResponse<? extends Problem>> {
    private static final IgniteLogger LOG = Loggers.forClass(JavaExceptionHandler.class);

    @Override
    public HttpResponse<? extends Problem> handle(HttpRequest request, Exception exception) {
        LOG.error("Unhandled exception", exception);
        return HttpProblemResponse.from(
                Problem.fromHttpCode(HttpCode.INTERNAL_SERVER_ERROR)
                        .detail(exception.getMessage())
        );
    }
}
