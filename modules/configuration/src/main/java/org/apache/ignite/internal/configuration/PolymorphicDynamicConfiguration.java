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

package org.apache.ignite.internal.configuration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.jetbrains.annotations.Nullable;

/**
 * This class represents a polymorphic configuration node.
 */
public abstract class PolymorphicDynamicConfiguration<VIEW extends InnerNode, CHANGE> extends DynamicConfiguration<VIEW, CHANGE> {
    /**
     * Constructor.
     *
     * @param prefix     Configuration prefix.
     * @param key        Configuration key.
     * @param rootKey    Root key.
     * @param changer    Configuration changer.
     * @param listenOnly Only adding listeners mode, without the ability to get or update the property value.
     */
    public PolymorphicDynamicConfiguration(
        List<String> prefix,
        String key,
        RootKey<?, ?> rootKey,
        DynamicConfigurationChanger changer,
        boolean listenOnly
    ) {
        super(prefix, key, rootKey, changer, listenOnly);
    }

    /** {@inheritDoc} */
    @Override protected void beforeRefreshValue(VIEW newValue, @Nullable VIEW oldValue) {
        if (oldValue == null || oldValue.schemaType() != newValue.schemaType()) {
            Map<String, ConfigurationProperty<?>> newMembers = new LinkedHashMap<>(members);

            if (oldValue != null)
                removeMembers(oldValue, newMembers);

            addMembers(newValue, newMembers);

            members = newMembers;
        }
    }

    /**
     * Removes members of the previous instance of polymorphic configuration.
     *
     * @param oldValue Old configuration value.
     * @param members Configuration members (leaves and nodes).
     */
    protected abstract void removeMembers(VIEW oldValue, Map<String, ConfigurationProperty<?>> members);

    /**
     * Adds members of the previous instance of polymorphic configuration.
     *
     * @param newValue New configuration value.
     * @param members Configuration members (leaves and nodes).
     */
    protected abstract void addMembers(VIEW newValue, Map<String, ConfigurationProperty<?>> members);

    /**
     * Add configuration member.
     *
     * @param members Configuration members (leaves and nodes).
     * @param member Configuration member (leaf or node).
     * @param <P> Type of member.
     */
    protected final <P extends ConfigurationProperty<?>> void addMember(
        Map<String, ConfigurationProperty<?>> members,
        P member
    ) {
        members.put(member.key(), member);
    }

    /**
     * Remove configuration member.
     *
     * @param members Configuration members (leaves and nodes).
     * @param member Configuration member (leaf or node).
     * @param <P> Type of member.
     */
    protected final <P extends ConfigurationProperty<?>> void removeMember(
        Map<String, ConfigurationProperty<?>> members,
        P member
    ) {
        members.remove(member.key());
    }

    /**
     * Returns specific configuration tree.
     *
     * @return Specific configuration tree.
     */
    public ConfigurationTree<VIEW, CHANGE> specificConfigTree() {
        // To work with polymorphic configuration.
        return this;
    }
}
