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

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.lang.IgniteException;

/**
 * Base configuration controller.
 */
public abstract class AbstractConfigurationController implements ResourceHolder {

    /** Presentation of the configuration. */
    private ConfigurationPresentation<String> cfgPresentation;

    public AbstractConfigurationController(ConfigurationPresentation<String> cfgPresentation) {
        this.cfgPresentation = cfgPresentation;
    }

    /**
     * Returns configuration.
     *
     * @return the presentation of configuration.
     */
    public HttpResponse<String> getConfiguration() {
        return plainTextResponse(cfgPresentation.represent());
    }

    /**
     * Returns configuration represented by path.
     *
     * @param path to represent a configuration.
     * @return system configuration represented by given path.
     */
    public HttpResponse<String> getConfigurationByPath(String path) {
        try {
            return plainTextResponse(cfgPresentation.representByPath(path));
        } catch (IllegalArgumentException ex) {
            throw new IgniteException(INTERNAL_ERR, ex);
        }
    }

    private static HttpResponse<String> plainTextResponse(String text) {
        return HttpResponse.ok(text).contentType(MediaType.TEXT_PLAIN);
    }

    /**
     * Updates configuration.
     *
     * @param updatedConfiguration the configuration to update.
     */
    public CompletableFuture<Void> updateConfiguration(String updatedConfiguration) {
        return cfgPresentation.update(updatedConfiguration);
    }

    @Override
    public void cleanResources() {
        cfgPresentation = null;
    }
}
