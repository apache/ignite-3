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
import java.util.Set;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.HttpCode;
import org.apache.ignite.internal.rest.problem.HttpProblemResponse;
import org.apache.ignite.lang.ErrorGroups.Table;
import org.apache.ignite.lang.IgniteException;

/**
 * Handles {@link IgniteException} and represents it as an application/problem+json response.
 */
@Singleton
@Requires(classes = {IgniteException.class, ExceptionHandler.class})
public class IgniteExceptionHandler implements ExceptionHandler<IgniteException, HttpResponse<? extends Problem>> {
    private static final Set<Integer> BAD_REQUEST_CODES = Set.of(Table.TABLE_NOT_FOUND_ERR);

    @Override
    public HttpResponse<? extends Problem> handle(HttpRequest request, IgniteException exception) {

        if (exception.getCause() instanceof IllegalArgumentException || BAD_REQUEST_CODES.contains(exception.code())) {
            return HttpProblemResponse.from(
                    Problem.fromIgniteException(exception, HttpCode.BAD_REQUEST).build()
            );
        }

        return HttpProblemResponse.from(
                Problem.fromIgniteException(exception, HttpCode.INTERNAL_SERVER_ERROR).build()
        );
    }
}
