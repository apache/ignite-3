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

package org.apache.ignite.configuration.internal;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.ignite.configuration.ConfigurationRegistry;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.internal.rest.JsonConverter;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.validation.Validator;

public class ConfigurationManager {

    private final ConfigurationRegistry confRegistry;

    private final Set<ConfigurationStorage> configurationStorages;

    public <A extends Annotation> ConfigurationManager(
        Collection<RootKey<?, ?>> rootKeys,
        Map<Class<A>, Set<Validator<A, ?>>> validators,
        Collection<ConfigurationStorage> configurationStorages
    ) {
        this.configurationStorages = Set.copyOf(configurationStorages);

        confRegistry = new ConfigurationRegistry(rootKeys, validators, configurationStorages);
    }

    public <A extends Annotation> ConfigurationManager(
        Collection<RootKey<?, ?>> rootKeys,
        Collection<ConfigurationStorage> configurationStorages
    ) {
        this(rootKeys, Collections.emptyMap(), configurationStorages);
    }

    /**
     * @param jsonStr
     */
    public void bootstrap(String jsonStr) throws InterruptedException, ExecutionException{
        JsonObject jsonCfg = JsonParser.parseString(jsonStr).getAsJsonObject();

        for (ConfigurationStorage configurationStorage : configurationStorages)
            confRegistry.change(Collections.emptyList(), JsonConverter.jsonSource(jsonCfg), configurationStorage).get();
    }

    /** */
    public ConfigurationRegistry configurationRegistry() {
        return confRegistry;
    }
}
