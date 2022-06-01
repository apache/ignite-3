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

package org.apache.ignite.internal.cluster.management.rest.exception.handler;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpResponseFactory;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.rest.api.ErrorResult;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Handles {@link IgniteInternalException} and represents it as a rest response.
 */
@Produces
@Singleton
@Requires(classes = {IgniteInternalException.class, ExceptionHandler.class})
public class IgniteInternalExceptionHandler implements ExceptionHandler<IgniteInternalException, HttpResponse<ErrorResult>> {

    @Override
    public HttpResponse<ErrorResult> handle(HttpRequest request, IgniteInternalException exception) {
        ErrorResult errorResult = new ErrorResult("ALREADY_INITIALIZED", exception.getMessage());
        return HttpResponseFactory.INSTANCE.status(HttpStatus.CONFLICT).body(errorResult);
    }
}
