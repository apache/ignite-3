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

package org.apache.ignite.internal.rest.health;

import static io.micronaut.health.HealthStatus.DOWN;
import static io.micronaut.health.HealthStatus.UP;
import static org.apache.ignite.internal.util.CompletableFutures.isCompletedSuccessfully;

import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import io.micronaut.management.health.indicator.annotation.Readiness;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.rest.cluster.JoinFutureProvider;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Health indicator that responds with the UP status when the node has joined the logical topology.
 */
@Singleton
@Readiness
public class NodeReadinessIndicator implements HealthIndicator {
    private final JoinFutureProvider joinFutureProvider;

    public NodeReadinessIndicator(JoinFutureProvider joinFutureProvider) {
        this.joinFutureProvider = joinFutureProvider;
    }

    @Override
    public Publisher<HealthResult> getResult() {
        HealthStatus healthStatus = isCompletedSuccessfully(joinFutureProvider.joinFuture()) ? UP : DOWN;
        return Flux.just(HealthResult.builder("node", healthStatus).build());
    }
}
