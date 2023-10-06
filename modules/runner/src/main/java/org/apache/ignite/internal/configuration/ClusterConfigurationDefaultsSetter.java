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

package org.apache.ignite.internal.configuration;

import static java.util.concurrent.CompletableFuture.failedFuture;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.ConfigurationDefaultsSetter;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.storage.RuntimeConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;

/**
 * Default configuration setter for cluster configuration.
 */
public class ClusterConfigurationDefaultsSetter implements ConfigurationDefaultsSetter, IgniteComponent {
    private static final String DEFAULT_PROVIDER_NAME = "default";

    private static final String DEFAULT_USERNAME = "ignite";

    private static final String DEFAULT_PASSWORD = "ignite";

    private final ConfigurationManager configurationManager;

    private final ConfigurationValidatorImpl configurationValidator;

    /**
     * Constructor.
     *
     * @param rootKeys Root keys.
     * @param generator Configuration tree generator.
     */
    public ClusterConfigurationDefaultsSetter(
            Collection<RootKey<?, ?>> rootKeys,
            ConfigurationTreeGenerator generator
    ) {
        configurationValidator = new ConfigurationValidatorImpl(generator, Set.of());
        configurationManager = new ConfigurationManager(
                rootKeys,
                new RuntimeConfigurationStorage(ConfigurationType.DISTRIBUTED),
                generator,
                configurationValidator
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<String> setDefaults(String hocon) {
        ConverterToMapVisitor visitor = ConverterToMapVisitor.builder()
                .includeInternal(true)
                .maskSecretValues(false)
                .build();

        ConfigurationRegistry configurationRegistry = configurationManager.configurationRegistry();

        return validateCfg(hocon)
                .thenCompose(v -> parseHocon(hocon))
                .thenCompose(
                        v -> setSecurityDefaults(configurationRegistry.getConfiguration(SecurityConfiguration.KEY))
                )
                .thenApply(
                        v -> HoconConverter.represent(configurationRegistry, List.of(), visitor).render(ConfigRenderOptions.concise())
                );
    }

    private CompletableFuture<Void> validateCfg(String hocon) {
        try {
            List<ValidationIssue> validationIssues = configurationValidator.validateHocon(hocon);
            if (validationIssues.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            } else {
                return failedFuture(new ConfigurationValidationException(validationIssues));
            }
        } catch (Exception e) {
            return failedFuture(new ConfigurationValidationException(e.getMessage()));
        }
    }

    private CompletableFuture<Void> parseHocon(String hocon) {
        try {
            Config config = ConfigFactory.parseString(hocon);
            ConfigurationSource hoconSource = HoconConverter.hoconSource(config.root());
            ConfigurationRegistry configurationRegistry = configurationManager.configurationRegistry();

            return configurationRegistry.change(hoconSource);
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    private static CompletableFuture<Void> setSecurityDefaults(SecurityConfiguration securityCfg) {
        if (securityCfg.authentication().providers().value().size() == 0) {
            return securityCfg.authentication().providers().change(change -> {
                change.create(DEFAULT_PROVIDER_NAME, providerChange -> {
                    providerChange.convert(BasicAuthenticationProviderChange.class)
                            .changeUsername(DEFAULT_USERNAME)
                            .changePassword(DEFAULT_PASSWORD);
                });
            });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public void start() {
        configurationManager.start();
    }

    @Override
    public void stop() throws Exception {
        configurationManager.stop();
    }
}
