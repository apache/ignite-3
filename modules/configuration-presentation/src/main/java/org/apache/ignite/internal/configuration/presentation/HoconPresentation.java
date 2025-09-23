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

package org.apache.ignite.internal.configuration.presentation;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.exception.ConfigurationApplyException;
import org.apache.ignite.internal.configuration.exception.ConfigurationParseException;
import org.apache.ignite.internal.configuration.exception.ConfigurationValidationIgniteException;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Representing the configuration as a string using HOCON as a converter to JSON.
 */
public class HoconPresentation implements ConfigurationPresentation<String> {
    /** Configuration registry. */
    private final ConfigurationRegistry registry;

    /**
     * Constructor.
     *
     * @param registry Configuration registry.
     */
    public HoconPresentation(ConfigurationRegistry registry) {
        this.registry = registry;
    }

    /** {@inheritDoc} */
    @Override
    public String represent() {
        return representByPath(null);
    }

    /** {@inheritDoc} */
    @Override
    public String representByPath(@Nullable String path) {
        return HoconConverter.represent(registry.superRoot(), path == null ? List.of() : ConfigurationUtil.split(path)).render(
                ConfigRenderOptions.concise());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> update(String cfgUpdate) {
        if (cfgUpdate.isBlank()) {
            return CompletableFuture.failedFuture(new ConfigurationParseException("Empty configuration."));
        }

        Config config;

        try {
            config = ConfigFactory.parseString(cfgUpdate);
        } catch (ConfigException.Parse e) {
            return CompletableFuture.failedFuture(new ConfigurationParseException("Failed to parse configuration.", e));
        }

        return registry.change(HoconConverter.hoconSource(config.root(), registry.keyIgnorer()))
                .exceptionally(e -> {
                    e = ExceptionUtils.unwrapCause(e);

                    if (e instanceof ConfigurationChangeException) {
                        e =  e.getCause();
                    }

                    if (e instanceof ConfigurationValidationException) {
                        throw new ConfigurationValidationIgniteException((ConfigurationValidationException) e);
                    }

                    throw new ConfigurationApplyException("Failed to apply configuration.", e);
                });
    }
}
