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
import io.micronaut.context.ApplicationContextConfiguration;
import io.micronaut.context.DefaultApplicationContext;
import io.micronaut.inject.BeanDefinitionReference;
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

        private final List<String> excludedPackages = new ArrayList<>();

        private final List<String> includedPackages = new ArrayList<>();

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
         * Excludes bean definitions from the specified packages. Bean definitions whose generated class name
         * starts with any of the given package prefixes will not be loaded into the context.
         *
         * <p>This is needed to prevent compile-time discovered beans from other Micronaut contexts
         * (e.g., REST module factories) from being loaded into this core DI context.
         *
         * @param packagePrefixes Package prefixes to exclude (e.g., {@code "org.apache.ignite.internal.rest"}).
         * @return This builder for chaining.
         */
        public Builder withExcludedPackages(String... packagePrefixes) {
            Collections.addAll(excludedPackages, packagePrefixes);
            return this;
        }

        /**
         * Restricts bean discovery to only the specified packages. Bean definitions whose generated class name
         * does not start with any of the given package prefixes will not be loaded into the context.
         *
         * @param packagePrefixes Package prefixes to include (e.g., {@code "org.apache.ignite.internal.di"}).
         * @return This builder for chaining.
         */
        public Builder withIncludedPackages(String... packagePrefixes) {
            Collections.addAll(includedPackages, packagePrefixes);
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

            if (excludedPackages.isEmpty() && includedPackages.isEmpty()) {
                return contextBuilder.start();
            }

            // Build the context manually so we can override bean discovery to filter
            // unwanted packages (e.g., REST module beans that conflict with core DI beans).
            // We replicate the singleton registration that ApplicationContextBuilder.build() does,
            // because DefaultApplicationContext(configuration) doesn't auto-register them.
            List<String> excluded = List.copyOf(excludedPackages);
            List<String> included = List.copyOf(includedPackages);

            @SuppressWarnings("rawtypes")
            ApplicationContext context = new DefaultApplicationContext((ApplicationContextConfiguration) contextBuilder) {
                @Override
                protected List<BeanDefinitionReference> resolveBeanDefinitionReferences() {
                    List<BeanDefinitionReference> refs = super.resolveBeanDefinitionReferences();

                    refs.removeIf(ref -> {
                        String name = ref.getBeanDefinitionName();

                        for (String pkg : excluded) {
                            if (name.startsWith(pkg + ".")) {
                                return true;
                            }
                        }

                        if (!included.isEmpty() && name.startsWith("org.apache.ignite.")) {
                            for (String pkg : included) {
                                if (name.startsWith(pkg + ".")) {
                                    return false;
                                }
                            }
                            return true;
                        }

                        return false;
                    });

                    return refs;
                }
            };

            for (Object singleton : singletons) {
                context.registerSingleton(singleton);
            }

            context.start();

            return context;
        }
    }
}
