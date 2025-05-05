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

import static java.lang.String.format;
import static org.apache.ignite.internal.configuration.hocon.HoconPrimitiveConfigurationSource.wrongTypeException;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.appendKey;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.join;

import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.configuration.ConfigurationWrongPolymorphicTypeIdException;
import org.apache.ignite.configuration.KeyIgnorer;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.jetbrains.annotations.Nullable;

/**
 * {@link ConfigurationSource} created from a HOCON object.
 */
class HoconObjectConfigurationSource implements ConfigurationSource {
    /**
     * Key that needs to be ignored by the source. Can be {@code null}.
     */
    private final String ignoredKey;

    /** Determines if key should be ignored. */
    private final KeyIgnorer keyIgnorer;

    /**
     * Current path inside the top-level HOCON object.
     */
    private final List<String> path;

    /**
     * HOCON object that this source has been created from.
     */
    private final ConfigObject hoconCfgObject;

    /**
     * Creates a {@link ConfigurationSource} from the given HOCON object.
     *
     * @param ignoredKey     Key that needs to be ignored by the source. Can be {@code null}.
     * @param keyIgnorer Determines if key should be ignored.
     * @param path           Current path inside the top-level HOCON object. Can be empty if the given {@code hoconCfgObject} is the
     *                       top-level object.
     * @param hoconCfgObject HOCON object.
     */
    HoconObjectConfigurationSource(
            @Nullable String ignoredKey,
            KeyIgnorer keyIgnorer,
            List<String> path,
            ConfigObject hoconCfgObject
    ) {
        this.ignoredKey = ignoredKey;
        this.keyIgnorer = keyIgnorer;
        this.path = path;
        this.hoconCfgObject = hoconCfgObject;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        throw wrongTypeException(clazz, path, -1);
    }

    @Override
    public void descend(ConstructableTreeNode node) {
        String injectedValueFieldName = node.injectedValueFieldName();

        if (injectedValueFieldName == null) {
            hoconCfgObject.forEach((key, value) -> parseConfigEntry(key, value, node));
        } else {
            assert hoconCfgObject.size() == 1; // User-friendly check must have been performed outside this method.

            ConfigValue value = hoconCfgObject.values().iterator().next();

            parseConfigEntry(injectedValueFieldName, value, node);
        }
    }

    private void parseConfigEntry(String key, ConfigValue hoconCfgValue, ConstructableTreeNode node) {
        if (keyIgnorer.shouldIgnore(join(appendKey(path, key)))) {
            return;
        }

        if (key.equals(ignoredKey)) {
            return;
        }

        try {
            switch (hoconCfgValue.valueType()) {
                case NULL:
                    node.construct(key, null, false);

                    break;

                case OBJECT: {
                    List<String> path = appendKey(this.path, key);

                    node.construct(
                            key,
                            new HoconObjectConfigurationSource(null, keyIgnorer, path, (ConfigObject) hoconCfgValue),
                            false
                    );

                    break;
                }

                case LIST: {
                    List<String> path = appendKey(this.path, key);

                    node.construct(key, new HoconListConfigurationSource(keyIgnorer, path, (ConfigList) hoconCfgValue), false);

                    break;
                }

                default: {
                    List<String> path = appendKey(this.path, key);

                    node.construct(key, new HoconPrimitiveConfigurationSource(keyIgnorer, path, hoconCfgValue), false);
                }
            }
        } catch (NoSuchElementException e) {
            if (path.isEmpty()) {
                throw new IllegalArgumentException(
                        format("'%s' configuration root doesn't exist", key), e
                );
            } else {
                throw new IllegalArgumentException(
                        format("'%s' configuration doesn't have the '%s' sub-configuration", join(path), key), e
                );
            }
        } catch (ConfigurationWrongPolymorphicTypeIdException e) {
            throw new IllegalArgumentException(
                    "Polymorphic configuration type is not correct: " + e.getMessage()
            );
        }
    }

    @Override
    public @Nullable String polymorphicTypeId(String fieldName) {
        ConfigValue typeId = hoconCfgObject.get(fieldName);

        if (typeId == null) {
            return null;
        }

        if (typeId.valueType() != ConfigValueType.STRING) {
            throw new IllegalArgumentException(
                    format("Invalid Polymorphic Type ID type. Expected %s, got %s", ConfigValueType.STRING, typeId.valueType())
            );
        }

        return (String) typeId.unwrapped();
    }
}
