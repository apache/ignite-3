/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.rest.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.rest.configuration.exception.ConfigPathUnrecognizedException;
import org.apache.ignite.internal.rest.configuration.exception.InvalidConfigFormatException;

/**
 * Base configuration controller.
 */
public class AbstractConfigurationController {
    /** CompletableFuture waiting timeout. */
    private static final int TIMEOUT_MINUTES = 1;

    /** Presentation of the configuration. */
    private final ConfigurationPresentation<String> cfgPresentation;

    public AbstractConfigurationController(ConfigurationPresentation<String> cfgPresentation) {
        this.cfgPresentation = cfgPresentation;
    }

    /**
     * Returns configuration.
     *
     * @return the presentation of configuration.
     */
    public String getConfiguration() {
        return this.cfgPresentation.represent();
    }

    /**
     * Returns configuration represented by path.
     *
     * @param path to represent a configuration.
     * @return system configuration represented by given path.
     */
    public String getConfigurationByPath(String path) {
        try {
            return cfgPresentation.representByPath(path);
        } catch (IllegalArgumentException ex) {
            throw new ConfigPathUnrecognizedException(ex);
        }
    }

    /**
     * Updates configuration.
     *
     * @param updatedConfiguration the configuration to update.
     */
    public void updateConfiguration(String updatedConfiguration) throws Throwable {
        try {
            cfgPresentation.update(updatedConfiguration).get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
        } catch (IllegalArgumentException | InterruptedException ex) {
            throw new InvalidConfigFormatException(ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof IllegalArgumentException) {
                throw new InvalidConfigFormatException(cause);
            }
            throw cause;
        }
    }
}
