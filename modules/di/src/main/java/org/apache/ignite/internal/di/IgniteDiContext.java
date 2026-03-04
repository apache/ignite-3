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

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.ApplicationContextBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Builds and manages a non-HTTP Micronaut {@link ApplicationContext} for core Ignite component wiring.
 *
 * <p>This context is separate from the REST module's HTTP-enabled Micronaut context.
 * It handles construction and dependency injection of {@link org.apache.ignite.internal.manager.IgniteComponent}
 * beans, while lifecycle management (start/stop) remains with {@link IgniteComponentLifecycleManager}.
 *
 * <p>Usage:
 * <pre>{@code
 * ApplicationContext ctx = IgniteDiContext.builder()
 *         .withSingleton(seedParams)
 *         .withPackages("org.apache.ignite.internal.app")
 *         .build();
 * }</pre>
 */
public final class IgniteDiContext {
    /**
     * Creates a new builder for the core DI context.
     *
     * @return A new builder instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for constructing the core DI {@link ApplicationContext}. */
    public static class Builder {
        private final List<Object> singletons = new ArrayList<>();

        private final List<String> packages = new ArrayList<>();

        /**
         * Registers a seed singleton that will be available for injection in the context.
         *
         * @param singleton The object to register as a singleton bean.
         * @return This builder for chaining.
         */
        public Builder withSingleton(Object singleton) {
            singletons.add(singleton);
            return this;
        }

        /**
         * Adds packages to scan for bean definitions.
         *
         * @param packageNames Package names to scan.
         * @return This builder for chaining.
         */
        public Builder withPackages(String... packageNames) {
            Collections.addAll(packages, packageNames);
            return this;
        }

        /**
         * Builds and starts the Micronaut {@link ApplicationContext}.
         *
         * @return The started application context.
         */
        public ApplicationContext build() {
            ApplicationContextBuilder contextBuilder = ApplicationContext.builder()
                    .deduceEnvironment(false)
                    .banner(false);

            if (!packages.isEmpty()) {
                contextBuilder.packages(packages.toArray(String[]::new));
            }

            if (!singletons.isEmpty()) {
                contextBuilder.singletons(singletons.toArray());
            }

            return contextBuilder.start();
        }
    }
}
