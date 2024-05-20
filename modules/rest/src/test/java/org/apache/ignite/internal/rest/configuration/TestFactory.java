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

package org.apache.ignite.internal.rest.configuration;

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;

/**
 * Factory that creates beans needed for unit tests.
 */
@Factory
public class TestFactory {
    /**
     * Creates test configuration registry.
     */
    @Singleton
    @Bean(preDestroy = "stopAsync")
    public ConfigurationRegistry configurationRegistry() {
        Validator<Value, Object> validator = new Validator<>() {
            /** {@inheritDoc} */
            @Override
            public void validate(Value annotation, ValidationContext<Object> ctx) {
                if (Objects.equals("error", ctx.getNewValue())) {
                    ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Error word"));
                }
            }
        };

        var generator = new ConfigurationTreeGenerator(TestRootConfiguration.KEY);

        var configurationRegistry = new ConfigurationRegistry(
                List.of(TestRootConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                generator,
                ConfigurationValidatorImpl.withDefaultValidators(generator, Set.of(validator))
        );

        assertThat(configurationRegistry.startAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());

        return configurationRegistry;
    }
}
