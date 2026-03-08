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

package org.apache.ignite.internal.di;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.ApplicationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.components.StartupPhase;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link IgniteComponentLifecycleManager}.
 */
public class IgniteComponentLifecycleManagerTest {
    private static final List<String> EVENTS = Collections.synchronizedList(new ArrayList<>());

    private ApplicationContext appContext;

    @AfterEach
    void tearDown() {
        EVENTS.clear();

        if (appContext != null && appContext.isRunning()) {
            appContext.close();
        }
    }

    @Test
    void componentsGroupedByPhase() {
        appContext = ApplicationContext.builder().start();

        IgniteComponentLifecycleManager manager = new IgniteComponentLifecycleManager(appContext);

        // TestComponentFactory produces one PHASE_1 and one PHASE_2 bean.
        assertThat(manager.componentsForPhase(StartupPhase.PHASE_1), hasSize(1));
        assertThat(manager.componentsForPhase(StartupPhase.PHASE_2), hasSize(1));
    }

    @Test
    void startPhaseStartsComponentsAndTracksOrder() {
        appContext = ApplicationContext.builder().start();

        IgniteComponentLifecycleManager manager = new IgniteComponentLifecycleManager(appContext);

        assertThat(manager.startedComponents(), is(empty()));

        assertThat(
                manager.startPhase(StartupPhase.PHASE_1, new ComponentContext()),
                willCompleteSuccessfully()
        );

        List<IgniteComponent> started = manager.startedComponents();
        assertThat(started, hasSize(manager.componentsForPhase(StartupPhase.PHASE_1).size()));
    }

    @Test
    void stopAllStopsInReverseStartOrder() {
        TrackingComponentA compA = new TrackingComponentA();
        TrackingComponentB compB = new TrackingComponentB();

        appContext = ApplicationContext.builder()
                .singletons(compA, compB)
                .start();

        IgniteComponentLifecycleManager manager = new IgniteComponentLifecycleManager(appContext);

        // Start all phases so all components (factory + tracking) are started.
        assertThat(
                manager.startPhase(StartupPhase.PHASE_1, new ComponentContext()),
                willCompleteSuccessfully()
        );

        // Record start order of tracking components.
        List<String> startOrder = EVENTS.stream()
                .filter(e -> e.startsWith("start:"))
                .map(e -> e.substring("start:".length()))
                .collect(Collectors.toList());
        assertEquals(2, startOrder.size(), "Expected exactly 2 tracking component start events");

        EVENTS.clear();

        assertThat(
                manager.stopAll(new ComponentContext()),
                willCompleteSuccessfully()
        );

        // Verify beforeNodeStop is called in reverse of start order.
        List<String> beforeStopOrder = EVENTS.stream()
                .filter(e -> e.startsWith("beforeNodeStop:"))
                .map(e -> e.substring("beforeNodeStop:".length()))
                .collect(Collectors.toList());

        List<String> stopOrder = EVENTS.stream()
                .filter(e -> e.startsWith("stop:"))
                .map(e -> e.substring("stop:".length()))
                .collect(Collectors.toList());

        List<String> expectedReverse = new ArrayList<>(startOrder);
        Collections.reverse(expectedReverse);

        assertEquals(expectedReverse, beforeStopOrder, "beforeNodeStop should be in reverse start order");
        assertEquals(expectedReverse, stopOrder, "stopAsync should be in reverse start order");

        // Verify all beforeNodeStop calls happen before any stopAsync calls.
        int lastBeforeStop = -1;
        int firstStop = Integer.MAX_VALUE;
        for (int i = 0; i < EVENTS.size(); i++) {
            if (EVENTS.get(i).startsWith("beforeNodeStop:")) {
                lastBeforeStop = i;
            }
            if (EVENTS.get(i).startsWith("stop:") && i < firstStop) {
                firstStop = i;
            }
        }
        assertTrue(lastBeforeStop < firstStop, "All beforeNodeStop calls should precede stopAsync calls");
    }

    @Test
    void multiPhaseStartAndStop() {
        appContext = ApplicationContext.builder().start();

        IgniteComponentLifecycleManager manager = new IgniteComponentLifecycleManager(appContext);

        assertThat(
                manager.startPhase(StartupPhase.PHASE_1, new ComponentContext()),
                willCompleteSuccessfully()
        );
        int phase1Count = manager.startedComponents().size();
        assertTrue(phase1Count > 0, "Phase 1 should have components");

        assertThat(
                manager.startPhase(StartupPhase.PHASE_2, new ComponentContext()),
                willCompleteSuccessfully()
        );
        int totalCount = manager.startedComponents().size();
        assertTrue(totalCount > phase1Count, "Phase 2 should add more started components");

        assertThat(
                manager.stopAll(new ComponentContext()),
                willCompleteSuccessfully()
        );
    }

    /** Base test component that records lifecycle events. */
    private abstract static class TrackingComponent implements IgniteComponent {
        private final String name;

        TrackingComponent(String name) {
            this.name = name;
        }

        @Override
        public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
            EVENTS.add("start:" + name);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void beforeNodeStop() {
            EVENTS.add("beforeNodeStop:" + name);
        }

        @Override
        public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
            EVENTS.add("stop:" + name);
            return CompletableFuture.completedFuture(null);
        }
    }

    /** Distinct type so Micronaut registers it as a separate bean. */
    private static class TrackingComponentA extends TrackingComponent {
        TrackingComponentA() {
            super("A");
        }
    }

    /** Distinct type so Micronaut registers it as a separate bean. */
    private static class TrackingComponentB extends TrackingComponent {
        TrackingComponentB() {
            super("B");
        }
    }
}
