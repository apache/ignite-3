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

package org.apache.ignite.configuration;

import java.io.Reader;
import java.io.Serializable;
import java.util.function.Consumer;

import com.google.gson.annotations.SerializedName;
import org.apache.ignite.configuration.extended.InitLocal;
import org.apache.ignite.configuration.extended.LocalConfiguration;
import org.apache.ignite.configuration.extended.Selectors;
import org.apache.ignite.configuration.internal.ConfigurationStorage;
import org.apache.ignite.configuration.internal.Configurator;
import org.apache.ignite.configuration.presentation.FormatConverter;
import org.apache.ignite.configuration.presentation.json.JsonConverter;

/** */
public class ConfigurationModule {
    static {
        try {
            Selectors.LOCAL_BASELINE_AUTO_ADJUST_ENABLED.select(null);
        }
        catch (Throwable ignored) {
            // No-op.
        }
    }

    private final ConfigurationStorage storage = new ConfigurationStorage() {

        @Override
        public <T extends Serializable> void save(String propertyName, T object) {

        }

        @Override
        public <T extends Serializable> T get(String propertyName) {
            return null;
        }

        @Override
        public <T extends Serializable> void listen(String key, Consumer<T> listener) {

        }
    };

    /** */
    private Configurator<LocalConfiguration> localConfigurator;

    /** */
    public void bootstrap(Reader confReader) {
        Configurator<LocalConfiguration> configurator = new Configurator<>(storage, LocalConfiguration::new);

        FormatConverter converter = new JsonConverter();

        ConfigWrapper wrapper = converter.convertFrom(confReader, ConfigWrapper.class);

        configurator.init(Selectors.LOCAL, wrapper.local);

        localConfigurator = configurator;
    }

    /** */
    public Configurator<LocalConfiguration> localConfigurator() {
        return localConfigurator;
    }

    /** */
    private static class ConfigWrapper {
        /** */
        @SerializedName("local")
        InitLocal local;
    }
}
