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

import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration container for {@link ConfigurationNotificationEvent}. Stores old and new nodes for the event, as well as their names.
 * Represents a stack, where current pair of nodes is at the top, and roots pair is at the bottom.
 */
class ConfigurationContainer {
    /** Parent container. */
    public final @Nullable ConfigurationContainer prev;

    /** {@link InnerNode#specificNode()} for the old value. */
    @Nullable
    private final Object oldNode;

    /** {@link InnerNode#specificNode()} for the new value. */
    @Nullable
    private final Object newNode;

    /** Old name, if the node is a part of named list. */
    private final String oldName;

    /** New name, if the node is a part of named list. */
    private final String newName;

    /**
     * Constructor.
     *
     * @param prev Parent container.
     * @param oldNode Old inner node value.
     * @param newNode New inner node value.
     * @param oldName Name of the old value, if part of the named list.
     * @param newName Name of the new value, if part of the named list.
     */
    ConfigurationContainer(
            @Nullable ConfigurationContainer prev,
            @Nullable InnerNode oldNode,
            @Nullable InnerNode newNode,
            @Nullable String oldName,
            @Nullable String newName
    ) {
        this.prev = prev;
        this.oldNode = oldNode == null ? null : oldNode.specificNode();
        this.newNode = newNode == null ? null : newNode.specificNode();
        this.oldName = oldName;
        this.newName = newName;
    }

    /**
     * Finds a node value that corresponds to a given {@code View} class.
     *
     * @param clazz Class instance of the {@code *View} type.
     * @param old Whether an old or a new value is asked.
     */
    public @Nullable <T> T find(Class<T> clazz, boolean old) {
        Object specificNode = node(old);

        if (clazz.isInstance(specificNode)) {
            return (T) specificNode;
        }

        return prev == null ? null : prev.find(clazz, old);
    }

    /**
     * Finds a name of the value that corresponds to a given {@code View} class.
     *
     * @param clazz Class instance of the {@code *View} type.
     * @param old Whether an old or a new value is asked.
     */
    public @Nullable String name(Class<?> clazz, boolean old) {
        Object specificNode = node(old);

        if (clazz.isInstance(specificNode)) {
            return name(old);
        }

        return prev == null ? null : prev.name(clazz, old);
    }

    private @Nullable String name(boolean old) {
        return old ? oldName : newName;
    }

    private @Nullable Object node(boolean old) {
        return old ? oldNode : newNode;
    }
}
