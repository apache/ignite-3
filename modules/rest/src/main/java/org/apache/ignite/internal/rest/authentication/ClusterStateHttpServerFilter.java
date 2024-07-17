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
    private RestManager restManager;

    public ClusterStateHttpServerFilter(RestManager restManager) {
        this.restManager = restManager;
    }

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
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

