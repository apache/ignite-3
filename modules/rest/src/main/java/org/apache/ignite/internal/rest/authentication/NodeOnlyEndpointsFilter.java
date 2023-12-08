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
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.rest.RestManager;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.HttpCode;
import org.apache.ignite.internal.rest.problem.HttpProblemResponse;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.reactivestreams.Publisher;

/** Filters out endpoints that are not allowed to be accessed before cluster is initialized. */
@Filter(Filter.MATCH_ALL_PATTERN)
@Requires(property = "ignite.endpoints.filter-non-initialized", value = "true", defaultValue = "false")
// TODO: https://issues.apache.org/jira/browse/IGNITE-19365
public class NodeOnlyEndpointsFilter implements HttpServerFilter {
    private final ClusterManagementGroupManager cmgMgr;

    private final RestManager restManager;

    public NodeOnlyEndpointsFilter(ClusterManagementGroupManager cmgMgr, RestManager restManager) {
        this.cmgMgr = cmgMgr;
        this.restManager = restManager;
    }

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        try {
            if (clusterNotInitialized() && restManager.pathDisabledForNotInitializedCluster(request.getPath())) {
                return Publishers.just(HttpProblemResponse.from(
                        Problem.fromHttpCode(HttpCode.CONFLICT)
                                .detail("Cluster is not initialized. Call /management/v1/cluster/init in order to initialize cluster.")
                                .build()
                ));
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IgniteException(Common.INTERNAL_ERR, e);
        }

        return chain.proceed(request);
    }

    private boolean clusterNotInitialized() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<ClusterState> stateFuture = cmgMgr.clusterState();
        return stateFuture == null || stateFuture.get(1, TimeUnit.SECONDS) == null;
    }
}

