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

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import io.micronaut.web.router.Router;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.RestManager;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.HttpCode;
import org.apache.ignite.internal.rest.problem.HttpProblemResponse;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * Filters out endpoints that are not allowed to be accessed.
 * */
@Filter(Filter.MATCH_ALL_PATTERN)
@Requires(property = "ignite.endpoints.filter-non-initialized", value = "true", defaultValue = "true")
public class ClusterStateHttpServerFilter implements HttpServerFilter, ResourceHolder {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterStateHttpServerFilter.class);

    private RestManager restManager;

    private final Router router;

    public ClusterStateHttpServerFilter(RestManager restManager, Router router) {
        this.restManager = restManager;
        this.router = router;
    }

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        // Guard against a possible race with cleanResources() during shutdown: the filter may still be invoked while
        // RestManager is being cleared.
        RestManager restManager = this.restManager;
        if (restManager == null) {
            return chain.proceed(request);
        }
        
        // If no route matches this path, skip state filtering and let the chain return 404. findAllClosest is the method used in the
        // RoutingInBoundHandler to find the route.
        if (router.findAllClosest(request).isEmpty()) {
            LOG.debug("No route found for request {}, skip availability check", request);
            return chain.proceed(request);
        }

        return Mono.just(restManager.pathAvailability(request.getPath())).<MutableHttpResponse<?>>flatMap(availability -> {
            if (!availability.isAvailable()) {
                return Mono.just(HttpProblemResponse.from(
                        Problem.fromHttpCode(HttpCode.CONFLICT)
                                .title(availability.unavailableTitle())
                                .detail(availability.unavailableReason())
                                .build()));
            }
            return Mono.empty();
        }).switchIfEmpty(Mono.from(chain.proceed(request)));
    }

    @Override
    public void cleanResources() {
        restManager = null;
    }
}

