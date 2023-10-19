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

package org.apache.ignite.internal.configuration.hocon;

import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.impl.ConfigImpl;
import java.util.List;
import org.apache.ignite.internal.configuration.ConfigurationConverter;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.ConverterToMapVisitor;

/**
 * Hocon converter.
 */
public class HoconConverter {
    /**
     * Converts configuration subtree to a HOCON {@link ConfigValue} instance.
     *
     * @param superRoot Super root instance.
     * @param path Path to the configuration subtree. Can be empty, can't be {@code null}.
     * @return {@link ConfigValue} instance that represents configuration subtree.
     * @throws IllegalArgumentException If {@code path} is not found in current configuration.
     */
    public static ConfigValue represent(
            SuperRoot superRoot,
            List<String> path
    ) {
        ConverterToMapVisitor visitor = ConverterToMapVisitor.builder()
                .includeInternal(false)
                .maskSecretValues(true)
                .build();
        return represent(superRoot, path, visitor);
    }

    /**
     * Converts configuration subtree to a HOCON {@link ConfigValue} instance.
     *
     * @param superRoot Super root instance.
     * @param path Path to the configuration subtree. Can be empty, can't be {@code null}.
     * @param visitor Visitor that will be used to convert configuration subtree.
     * @return {@link ConfigValue} instance that represents configuration subtree.
     * @throws IllegalArgumentException If {@code path} is not found in current configuration.
     */
    public static ConfigValue represent(
            SuperRoot superRoot,
            List<String> path,
            ConfigurationVisitor<?> visitor
    ) {
        Object res = ConfigurationConverter.convert(superRoot, path, visitor);
        return ConfigImpl.fromAnyRef(res, null);
    }

    /**
     * Returns HOCON-based configuration source.
     *
     * @param hoconCfg HOCON that has to be converted to the configuration source.
     * @return HOCON-based configuration source.
     */
    public static ConfigurationSource hoconSource(ConfigObject hoconCfg) {
        return new HoconObjectConfigurationSource(null, List.of(), hoconCfg);
    }
}
