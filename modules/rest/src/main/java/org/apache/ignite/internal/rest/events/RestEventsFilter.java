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

package org.apache.ignite.internal.rest.events;

import io.micronaut.context.annotation.Requires;
import io.micronaut.core.order.Ordered;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import io.micronaut.http.filter.ServerFilterPhase;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * HTTP filter that logs REST API request start and finish events.
 */
@Filter(Filter.MATCH_ALL_PATTERN)
@Requires(property = "ignite.endpoints.rest-events", value = "true", defaultValue = "true")
public class RestEventsFilter implements HttpServerFilter, ResourceHolder, Ordered {
    private RestEvents restEvents;

    /** Constructor. */
    public RestEventsFilter(RestEvents restEvents) {
        this.restEvents = restEvents;
    }

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        restEvents.logRequestStarted(request);

        return Mono.from(chain.proceed(request))
                .doOnSuccess(response -> restEvents.logRequestFinished(request, response))
                .doOnError(throwable -> restEvents.logRequestError(request, throwable));
    }

    @Override
    public int getOrder() {
        return ServerFilterPhase.SECURITY.after();
    }

    @Override
    public void cleanResources() {
        restEvents = null;
    }
}
