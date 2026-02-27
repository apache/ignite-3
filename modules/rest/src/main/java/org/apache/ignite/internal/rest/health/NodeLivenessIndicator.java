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

import static io.micronaut.health.HealthStatus.UP;

import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import io.micronaut.management.health.indicator.annotation.Liveness;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Health indicator that always responds with the UP status.
 */
@Singleton
@Liveness
public class NodeLivenessIndicator implements HealthIndicator {
    private static final IgniteLogger LOG = Loggers.forClass(NodeLivenessIndicator.class);

    @Override
    public Publisher<HealthResult> getResult() {
        LOG.debug("Liveness check is UP.");
        return Flux.just(HealthResult.builder("node", UP).build());
    }
}
