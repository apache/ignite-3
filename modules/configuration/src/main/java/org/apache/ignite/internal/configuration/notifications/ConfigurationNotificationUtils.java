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

package org.apache.ignite.internal.configuration.notifications;

import static java.util.Collections.emptyIterator;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.internal.configuration.ConfigurationNode;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicProperty;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.jetbrains.annotations.Nullable;

/**
 * Useful class for notifying configuration listeners.
 */
class ConfigurationNotificationUtils {
    /**
     * Private constructor.
     */
    private ConfigurationNotificationUtils() {
        // No op.
    }

    /**
     * Returns the dynamic property of the leaf.
     *
     * @param dynamicConfig Dynamic configuration.
     * @param nodeName Name of the child node.
     * @return Dynamic property of a leaf or {@code null} if the leaf does not exist.
     */
    static @Nullable DynamicProperty<Serializable> dynamicProperty(DynamicConfiguration<InnerNode, ?> dynamicConfig, String nodeName) {
        return (DynamicProperty<Serializable>) dynamicConfig.members().get(nodeName);
    }

    /**
     * Returns the dynamic configuration of the child node.
     *
     * @param dynamicConfig Dynamic configuration.
     * @param nodeName Name of the child node.
     * @return Dynamic configuration of the child node or {@code null} if the child node does not exist.
     */
    static @Nullable DynamicConfiguration<InnerNode, ?> dynamicConfig(DynamicConfiguration<InnerNode, ?> dynamicConfig, String nodeName) {
        return (DynamicConfiguration<InnerNode, ?>) dynamicConfig.members().get(nodeName);
    }

    /**
     * Returns the named dynamic configuration of the child node.
     *
     * @param dynamicConfig Dynamic configuration.
     * @param nodeName Name of the child node.
     * @return Named dynamic configuration of the child node or {@code null} if the child node does not exist.
     */
    static @Nullable NamedListConfiguration<?, InnerNode, ?> namedDynamicConfig(
            DynamicConfiguration<InnerNode, ?> dynamicConfig,
            String nodeName
    ) {
        return (NamedListConfiguration<?, InnerNode, ?>) dynamicConfig.members().get(nodeName);
    }

    /**
     * Null-safe version of {@link ConfigurationNode#listeners(long)}.
     *
     * @param node Configuration tree node.
     * @param notificationNumber Configuration notification listener number.
     */
    static <T> Iterator<ConfigurationListener<T>> listeners(@Nullable ConfigurationNode<T> node, long notificationNumber) {
        return node == null ? emptyIterator() : node.listeners(notificationNumber);
    }

    /**
     * Null-safe version of {@link NamedListConfiguration#extendedListeners(long)}.
     *
     * @param node Named list configuration.
     * @param notificationNumber Configuration notification listener number.
     */
    static <T> Iterator<ConfigurationNamedListListener<T>> extendedListeners(
            @Nullable NamedListConfiguration<?, T, ?> node,
            long notificationNumber
    ) {
        return node == null ? emptyIterator() : node.extendedListeners(notificationNumber);
    }

    /**
     * Returns the dynamic configuration of the {@link NamedListConfiguration#any any} node.
     *
     * @param namedConfig Dynamic configuration.
     * @return Dynamic configuration of the "any" node.
     */
    static @Nullable DynamicConfiguration<InnerNode, ?> any(@Nullable NamedListConfiguration<?, InnerNode, ?> namedConfig) {
        return namedConfig == null ? null : (DynamicConfiguration<InnerNode, ?>) namedConfig.any();
    }

    /**
     * Merge {@link NamedListConfiguration#any "any"} configurations.
     *
     * @param anyConfigs Current {@link NamedListConfiguration#any "any"} configurations.
     * @param anyConfig  New {@link NamedListConfiguration#any "any"} configuration.
     * @return Merged {@link NamedListConfiguration#any "any"} configurations.
     */
    static Iterable<DynamicConfiguration<InnerNode, ?>> mergeAnyConfigs(
            Iterable<DynamicConfiguration<InnerNode, ?>> anyConfigs,
            @Nullable DynamicConfiguration<InnerNode, ?> anyConfig
    ) {
        if (anyConfig == null) {
            return anyConfigs;
        }

        return () -> {
            Iterator<DynamicConfiguration<InnerNode, ?>> innerIterator = anyConfigs.iterator();

            return new Iterator<>() {
                boolean finished = false;
                @Override
                public boolean hasNext() {
                    return innerIterator.hasNext() || !finished;
                }

                @Override
                public DynamicConfiguration<InnerNode, ?> next() {
                    if (finished) {
                        throw new NoSuchElementException();
                    }

                    if (innerIterator.hasNext()) {
                        return innerIterator.next();
                    }

                    finished = true;

                    return anyConfig;
                }
            };
        };
    }
}
