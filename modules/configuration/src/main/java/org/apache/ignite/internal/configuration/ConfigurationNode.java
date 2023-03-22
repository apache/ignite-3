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

package org.apache.ignite.internal.configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.ignite.configuration.ConfigurationListenOnlyException;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.internal.configuration.direct.DirectPropertyProxy;
import org.apache.ignite.internal.configuration.direct.KeyPathNode;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.util.KeyNotFoundException;
import org.jetbrains.annotations.Nullable;

/**
 * Super class for dynamic configuration tree nodes. Has all common data and value retrieving algorithm in it.
 */
public abstract class ConfigurationNode<VIEWT> implements ConfigurationProperty<VIEWT> {
    /** Listeners of property update. */
    private final ConfigurationListenerHolder<ConfigurationListener<VIEWT>> updateListeners = new ConfigurationListenerHolder<>();

    /** Full path to the current node. */
    protected final List<String> keys;

    /** Name of the current node. Same as last element of {@link #keys}. */
    protected final String key;

    /** Root key instance for the current trees root. */
    protected final RootKey<?, ?> rootKey;

    /** Configuration changer instance to get latest value of the root. */
    protected final DynamicConfigurationChanger changer;

    /** Only adding listeners mode, without the ability to get or update the property value. */
    protected final boolean listenOnly;

    /**
     * Cached value of current trees root. Useful to determine whether you have the latest configuration value or not.
     */
    private volatile TraversableTreeNode cachedRootNode;

    /** Cached configuration value. Immutable. */
    private VIEWT val;

    /**
     * Validity flag. Configuration is declared invalid if it's a part of named list configuration and corresponding entry is already
     * removed.
     */
    private boolean invalid;

    /**
     * Constructor.
     *
     * @param keys Full path to the current node.
     * @param key Name of the current node. Same as last element of {@link #keys}.
     * @param rootKey Root key.
     * @param changer Configuration changer.
     * @param listenOnly Only adding listeners mode, without the ability to get or update the property value.
     */
    protected ConfigurationNode(
            List<String> keys,
            String key,
            RootKey<?, ?> rootKey,
            DynamicConfigurationChanger changer,
            boolean listenOnly
    ) {
        this.keys = keys;
        this.key = key;
        this.rootKey = rootKey;
        this.changer = changer;
        this.listenOnly = listenOnly;

        assert Objects.equals(rootKey.key(), keys.get(0));
    }

    /** {@inheritDoc} */
    @Override
    public void listen(ConfigurationListener<VIEWT> listener) {
        updateListeners.addListener(listener, changer.notificationCount());
    }

    /** {@inheritDoc} */
    @Override
    public void stopListen(ConfigurationListener<VIEWT> listener) {
        updateListeners.removeListener(listener);
    }

    /**
     * Returns an iterator of the listeners for the {@code notificationNumber} (were added for and before it).
     *
     * <p>NOTE: {@link Iterator#remove} - not supported.
     *
     * @param notificationNumber Configuration notification listener number.
     */
    public Iterator<ConfigurationListener<VIEWT>> listeners(long notificationNumber) {
        return updateListeners.listeners(notificationNumber);
    }

    /**
     * Returns latest value of the configuration or throws exception.
     *
     * @return Latest configuration value.
     * @throws NoSuchElementException           If configuration is a part of already deleted named list configuration entry.
     * @throws ConfigurationListenOnlyException If there was an attempt to get or update a property value in {@link #listenOnly listen-only}
     *                                          mode.
     */
    protected final VIEWT refreshValue() throws NoSuchElementException {
        TraversableTreeNode newRootNode = changer.getRootNode(rootKey);
        TraversableTreeNode oldRootNode = cachedRootNode;

        // 'invalid' and 'val' visibility is guaranteed by the 'cachedRootNode' volatile read
        if (invalid) {
            throw noSuchElementException();
        } else if (listenOnly) {
            throw listenOnlyException();
        }

        if (oldRootNode == newRootNode) {
            return val;
        }

        try {
            VIEWT newVal = ConfigurationUtil.find(keys.subList(1, keys.size()), newRootNode, true);

            synchronized (this) {
                if (cachedRootNode == oldRootNode) {
                    beforeRefreshValue(newVal, val);

                    val = newVal;

                    cachedRootNode = newRootNode;

                    return newVal;
                } else {
                    if (invalid) {
                        throw noSuchElementException();
                    }

                    return val;
                }
            }
        } catch (KeyNotFoundException e) {
            synchronized (this) {
                invalid = true;

                cachedRootNode = newRootNode;
            }

            throw noSuchElementException();
        }
    }

    /**
     * Returns exception instance with a proper error message.
     *
     * @return Exception instance with a proper error message.
     */
    private NoSuchElementException noSuchElementException() {
        return new NoSuchElementException(ConfigurationUtil.join(keys));
    }

    /**
     * Callback from {@link #refreshValue()} that's called right before the update. Synchronized.
     *
     * @param newValue New configuration value.
     * @param oldValue Old configuration value.
     */
    protected void beforeRefreshValue(VIEWT newValue, @Nullable VIEWT oldValue) {
        // No-op.
    }

    /**
     * Returns Exception if there was an attempt to get or update a property value in {@link #listenOnly listen-only} mode.
     *
     * @return Exception if there was an attempt to get or update a property value in {@link #listenOnly listen-only} mode.
     */
    protected final ConfigurationListenOnlyException listenOnlyException() {
        return new ConfigurationListenOnlyException("`any` configuration node can only be used for listeners [keys=" + keys + ']');
    }

    /**
     * Converts {@link #keys} into a list of {@link KeyPathNode}. Result is used in implementations of {@link DirectPropertyProxy}.
     */
    protected final List<KeyPathNode> keyPath() {
        if (listenOnly) {
            throw listenOnlyException();
        }

        ConfigurationVisitor<List<KeyPathNode>> visitor = new ConfigurationVisitor<>() {
            /** List with the result. */
            private List<KeyPathNode> res = new ArrayList<>(keys.size());

            /** Current index. */
            private int idx = 1;

            /** {@inheritDoc} */
            @Nullable
            @Override
            public List<KeyPathNode> visitLeafNode(String key, Serializable val) {
                res.add(new KeyPathNode(key));

                return res;
            }

            /** {@inheritDoc} */
            @Nullable
            @Override
            public List<KeyPathNode> visitInnerNode(String key, InnerNode node) {
                res.add(new KeyPathNode(key));

                if (keys.size() == idx) {
                    return res;
                }

                node.traverseChild(keys.get(idx++), this, true);

                return res;
            }

            /** {@inheritDoc} */
            @Nullable
            @Override
            public List<KeyPathNode> visitNamedListNode(String key, NamedListNode node) {
                res.add(new KeyPathNode(key));

                if (keys.size() == idx) {
                    return res;
                }

                InnerNode innerNode = node.getInnerNode(keys.get(idx++));

                if (innerNode == null) {
                    throw noSuchElementException();
                }

                // This is important, node is added as a resolved named list entry here.
                res.add(new KeyPathNode(innerNode.internalId().toString(), false));

                if (keys.size() == idx) {
                    return res;
                }

                innerNode.traverseChild(keys.get(idx++), this, true);

                return res;
            }
        };

        return changer.getRootNode(rootKey).accept(keys.get(0), visitor);
    }

    /**
     * Returns current value of the configuration.
     */
    @Nullable
    protected final VIEWT currentValue() {
        return val;
    }
}
