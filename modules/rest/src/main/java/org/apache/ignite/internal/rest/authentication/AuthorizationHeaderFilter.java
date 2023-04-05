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

package org.apache.ignite.internal.rest.authentication;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import io.micronaut.http.filter.ServerFilterPhase;
import org.reactivestreams.Publisher;

/**
 * Implementation of {@link HttpServerFilter}. Checks {@link HttpRequest}
 * and adds empty {@link io.micronaut.http.HttpHeaders#AUTHORIZATION}
 * header if it's absent. We need this workaround, because Micronaut always returns 403,
 * when the authentication is enabled and the request doesn't have
 * {@link io.micronaut.http.HttpHeaders#AUTHORIZATION} header.
 */
@Filter(Filter.MATCH_ALL_PATTERN)
public class AuthorizationHeaderFilter implements HttpServerFilter {

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        return chain.proceed(addAuthorizationHeaderIfAbsent(request));
    }

    @Override
    public int getOrder() {
        return ServerFilterPhase.SECURITY.before();
    }

    private static HttpRequest<?> addAuthorizationHeaderIfAbsent(HttpRequest<?> request) {
        if (request.getHeaders().getAuthorization().isPresent()) {
            return request;
        } else {
            return request.mutate().basicAuth("", "");
        }
    }
}
