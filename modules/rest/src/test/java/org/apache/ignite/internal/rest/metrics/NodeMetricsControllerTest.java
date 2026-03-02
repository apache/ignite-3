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

package org.apache.ignite.internal.rest.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSnapshot;
import org.apache.ignite.internal.metrics.messaging.MetricMessaging;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

@MicronautTest
@Property(name = "ignite.endpoints.filter-non-initialized", value = "false")
@Property(name = "ignite.endpoints.rest-events", value = "false")
@Property(name = "micronaut.security.enabled", value = "false")
class NodeMetricsControllerTest extends BaseIgniteAbstractTest {
    @Inject
    @Client("/management/v1/metric/node")
    private HttpClient client;

    private final MetricManager metricManager = mock(MetricManager.class);

    private final MetricMessaging metricMessaging = mock(MetricMessaging.class);

    @Bean
    @Replaces(MetricRestFactory.class)
    MetricRestFactory metricRestFactory() {
        return new MetricRestFactory(metricManager, metricMessaging);
    }

    @Test
    void emptyMetricSet() {
        when(metricManager.metricSnapshot()).thenReturn(new MetricSnapshot(Map.of("setName", new MetricSet("setName", Map.of())), 1L));

        assertThat(client.toBlocking().retrieve("/set"), is("[{\"name\":\"setName\",\"metrics\":[]}]"));
    }
}
