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

package org.apache.ignite.internal.rest.api;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Error;
import org.apache.ignite.internal.rest.constants.HttpCode;
import org.apache.ignite.internal.rest.problem.HttpProblemResponse;

/**
 * Controller that handles general errors.
 */
@Controller
public class GeneralErrorsController {
    /**
     * 404 Not Found.
     */
    @Error(status = HttpStatus.NOT_FOUND, global = true)
    public HttpResponse<? extends Problem> notFound(HttpRequest<?> request) {
        return HttpProblemResponse.from(
                Problem.fromHttpCode(HttpCode.NOT_FOUND)
                        .detail("Requested resource not found: " + request.getPath())
        );
    }

    /**
     * 405 Method Not Allowed.
     */
    @Error(status = HttpStatus.METHOD_NOT_ALLOWED, global = true)
    public HttpResponse<? extends Problem> methodNotAllowed(HttpRequest<?> request) {
        return HttpProblemResponse.from(
                Problem.fromHttpCode(HttpCode.METHOD_NOT_ALLOWED)
                        .detail("Method not allowed: " + request.getMethodName())
        );
    }

    /**
     * 415 Unsupported Media Type.
     */
    @Error(status = HttpStatus.UNSUPPORTED_MEDIA_TYPE, global = true)
    public HttpResponse<? extends Problem> unsupportedMediaType(HttpRequest<?> request) {
        return HttpProblemResponse.from(
                Problem.fromHttpCode(HttpCode.UNSUPPORTED_MEDIA_TYPE)
                        .detail("Unsupported media type: " + request.getContentType().map(MediaType::getType).orElse(null))
        );
    }

    /**
     * 522 Connection timed out.
     */
    @Error(status = HttpStatus.CONNECTION_TIMED_OUT, global = true)
    public HttpResponse<? extends Problem> connectionTimedOut(HttpRequest<?> request) {
        return HttpProblemResponse.from(
                Problem.fromHttpCode(HttpCode.CONNECTION_TIMED_OUT)
                        .detail("Connection timed out: " + request.getPath())
        );
    }
}
