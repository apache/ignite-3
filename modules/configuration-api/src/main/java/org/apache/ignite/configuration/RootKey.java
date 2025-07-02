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

package org.apache.ignite.configuration;

import java.util.Objects;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.tostring.S;

/**
 * Configuration root selector.
 *
 * @param <T> Type of the configuration tree described by the root key.
 * @param <VIEWT> Type of the immutable snapshot view associated with the tree.
 * @param <CHANGET> Type of the object that is used to change the configuration.
 */
public final class RootKey<T extends ConfigurationTree<? super VIEWT, ? super CHANGET>, VIEWT, CHANGET extends VIEWT> {
    /** Name of the configuration root. */
    private final String rootName;

    /** Configuration type of the root. */
    private final ConfigurationType storageType;

    /** Schema class for the root. */
    private final Class<?> schemaClass;

    /** Marked with {@link ConfigurationExtension} and internal = {@code true}. */
    private final boolean internal;

    /**
     * Constructor with explicit fields values.
     */
    public RootKey(String rootName, ConfigurationType storageType, Class<?> schemaClass, boolean internal) {
        this.rootName = Objects.requireNonNull(rootName, "rootName");
        this.storageType = storageType;
        this.schemaClass = schemaClass;
        this.internal = internal;
    }

    /**
     * Returns the name of the configuration root.
     *
     * @return Name of the configuration root.
     */
    public String key() {
        return rootName;
    }

    /**
     * Returns the configuration type of the root.
     *
     * @return Configuration type of the root.
     */
    public ConfigurationType type() {
        return storageType;
    }

    /**
     * Returns the schema class for the root.
     *
     * @return Schema class for the root.
     */
    public Class<?> schemaClass() {
        return schemaClass;
    }

    /**
     * Check if the root configuration is marked with {@link ConfigurationExtension}
     * and {@link ConfigurationExtension#internal()} is {@code true}.
     *
     * @return {@code true} if the root configuration is internal.
     */
    public boolean internal() {
        return internal;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(RootKey.class, this);
    }
}
