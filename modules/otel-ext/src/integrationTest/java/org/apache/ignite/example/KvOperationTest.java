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

package org.apache.ignite.example;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tracing.TracingManager.getSpanManager;
import static org.apache.ignite.internal.tracing.TracingManager.rootSpan;
import static org.apache.ignite.internal.tracing.TracingManager.span;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Map;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.tracing.NoopSpanManager;
import org.apache.ignite.internal.tracing.TraceSpan;
import org.apache.ignite.internal.tracing.configuration.TracingConfiguration;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for get operation.
 */
public class KvOperationTest extends ClusterPerClassIntegrationTest {
    @BeforeAll
    @Override
    protected void beforeAll(TestInfo testInfo) {
        super.beforeAll(testInfo);

        createZoneAndTable(zoneName(DEFAULT_TABLE_NAME), DEFAULT_TABLE_NAME, 1, 1);

        insertPeople(DEFAULT_TABLE_NAME, new Person(0, "0", 10.0));

        assertSame(getSpanManager(), NoopSpanManager.INSTANCE);
    }

    @Test
    void delayTracing() {
        IgniteImpl ignite = CLUSTER.aliveNode();

        assertThat(
                ignite.clusterConfiguration().getConfiguration(TracingConfiguration.KEY).change(change -> change.changeRatio(1.0d)),
                willCompleteSuccessfully()
        );

        assertNotSame(getSpanManager(), NoopSpanManager.INSTANCE);

        try (TraceSpan parentSpan = rootSpan("try-span")) {
            try (TraceSpan ignored = span("childSpan")) {
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        rootSpan("closure-span", (parentSpan) -> {
            span("childSpan", (span) -> {
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            return null;
        });
    }

    @Test
    void kvGetWithTracing() {
        IgniteImpl ignite = CLUSTER.aliveNode();

        assertThat(
                ignite.clusterConfiguration().getConfiguration(TracingConfiguration.KEY).change(change -> change.changeRatio(1.0d)),
                willCompleteSuccessfully()
        );

        assertNotSame(getSpanManager(), NoopSpanManager.INSTANCE);

        KeyValueView<Tuple, Tuple> keyValueView = ignite.tables().table(DEFAULT_TABLE_NAME).keyValueView();

        // Warm-up
        try (TraceSpan parentSpan = rootSpan("WarmSpan")) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        Tuple key = Tuple.create(Map.of("id", 0));

        var start = System.nanoTime();

        try (TraceSpan parentSpan = rootSpan("kvGetOperation")) {
            try (TraceSpan childSpan = span("kvGet")) {
                // No-op.
            }
        }

        System.out.println(">>> " + (System.nanoTime() - start) / 1000L);

        try (TraceSpan ignored = rootSpan("kvGetOperation")) {
            keyValueView.get(null, key);
        }
    }

    @Override
    protected int initialNodes() {
        return 1;
    }
}
