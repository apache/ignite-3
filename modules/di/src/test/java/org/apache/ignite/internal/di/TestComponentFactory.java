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

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * Test factory that produces {@link IgniteComponent} beans annotated with different {@link IgniteStartupPhase}s.
 */
@Factory
class TestComponentFactory {
    @Bean
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    TestPhase1Component phase1Component() {
        return new TestPhase1Component();
    }

    @Bean
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    TestPhase2Component phase2Component() {
        return new TestPhase2Component();
    }

    /** Test component for Phase 1. */
    static class TestPhase1Component implements IgniteComponent {
        @Override
        public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /** Test component for Phase 2. */
    static class TestPhase2Component implements IgniteComponent {
        @Override
        public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
