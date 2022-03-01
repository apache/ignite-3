/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.rest.api;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for defining REST API request handlers.
 */
public interface RequestHandler {
    /**
     * Handles the given request and writes the response into the provided {@link RestApiHttpResponse}.
     *
     * @param request REST API request
     * @param response REST API response
     * @return Future that resolves as soon as the response is ready to be sent to the caller. It may return either a new response object
     *         or the same as the provided via the {@code response} parameter.
     */
    CompletableFuture<RestApiHttpResponse> handle(RestApiHttpRequest request, RestApiHttpResponse response);
}
