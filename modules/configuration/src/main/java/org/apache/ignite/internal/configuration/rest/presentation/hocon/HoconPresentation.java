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

package org.apache.ignite.internal.configuration.rest.presentation.hocon;

import static com.typesafe.config.ConfigFactory.parseString;
import static com.typesafe.config.ConfigRenderOptions.concise;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.split;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.rest.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Representing the configuration as a string using HOCON as a converter to JSON.
 */
public class HoconPresentation implements ConfigurationPresentation<String> {
    /** Configuration registry. */
    private final ConfigurationRegistry registry;
    private static final IgniteLogger LOG = Loggers.forClass(HoconPresentation.class);

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
        String render = HoconConverter.represent(registry, path == null ? List.of() : split(path)).render(concise());
        LOG.warn("represent {}", render);
        return render;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> update(String cfgUpdate) {
        if (cfgUpdate.isBlank()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Empty configuration"));
        }

        Config config;

        try {
            config = parseString(cfgUpdate);
        } catch (ConfigException.Parse e) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(e));
        }

        return registry.change(HoconConverter.hoconSource(config.root()))
                .exceptionally(e -> {
                    if (e instanceof CompletionException) {
                        e = e.getCause();
                    }

                    if (e instanceof IllegalArgumentException) {
                        throw (RuntimeException) e;
                    } else if (e instanceof ConfigurationValidationException) {
                        throw (RuntimeException) e;
                    } else if (e instanceof ConfigurationChangeException) {
                        throw (RuntimeException) e.getCause();
                    } else {
                        throw new IgniteException(e);
                    }
                });
    }
}
