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
        final ErrorResult errorResult = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", exception.getCause().getMessage());
        return HttpResponse.badRequest().body(errorResult);
    }
}
