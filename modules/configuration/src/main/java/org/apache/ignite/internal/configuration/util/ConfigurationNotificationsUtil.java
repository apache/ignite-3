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

package org.apache.ignite.internal.configuration.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicProperty;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.innerNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.leafNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.namedListNodeVisitor;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.apache.ignite.internal.util.CollectionUtils.view;

/** */
public class ConfigurationNotificationsUtil {
    /**
     * Recursively notifies all public configuration listeners, accumulating resulting futures in {@code futures} list.
     *
     * @param oldInnerNode Old configuration values root.
     * @param newInnerNode New configuration values root.
     * @param cfgNode Public configuration tree node corresponding to the current inner nodes.
     * @param storageRevision Storage revision.
     * @param futures Write-only list of futures to accumulate results.
     */
    public static void notifyListeners(
        InnerNode oldInnerNode,
        InnerNode newInnerNode,
        DynamicConfiguration<InnerNode, ?> cfgNode,
        long storageRevision,
        List<CompletableFuture<?>> futures
    ) {
        notifyListeners(oldInnerNode, newInnerNode, cfgNode, storageRevision, futures, List.of());
    }

    /**
     * Recursively notifies all public configuration listeners, accumulating resulting futures in {@code futures} list.
     *
     * @param oldInnerNode Old configuration values root.
     * @param newInnerNode New configuration values root.
     * @param cfgNode Public configuration tree node corresponding to the current inner nodes.
     * @param storageRevision Storage revision.
     * @param futures Write-only list of futures to accumulate results.
     * @param anyConfigs Current {@link NamedListConfiguration#any "any"} configurations.
     */
    private static void notifyListeners(
        InnerNode oldInnerNode,
        InnerNode newInnerNode,
        DynamicConfiguration<InnerNode, ?> cfgNode,
        long storageRevision,
        List<CompletableFuture<?>> futures,
        Iterable<DynamicConfiguration<InnerNode, ?>> anyConfigs
    ) {
        assert !(cfgNode instanceof NamedListConfiguration);

        if (oldInnerNode == null || oldInnerNode == newInnerNode)
            return;

        for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
            notifyPublicListeners(
                anyConfig.listeners(),
                oldInnerNode,
                newInnerNode,
                storageRevision,
                futures,
                ConfigurationListener::onUpdate
            );
        }

        notifyPublicListeners(
            cfgNode.listeners(),
            oldInnerNode,
            newInnerNode,
            storageRevision,
            futures,
            ConfigurationListener::onUpdate
        );

        oldInnerNode.traverseChildren(new ConfigurationVisitor<Void>() {
            /** {@inheritDoc} */
            @Override public Void visitLeafNode(String key, Serializable oldLeaf) {
                Serializable newLeaf = newInnerNode.traverseChild(key, leafNodeVisitor(), true);

                if (newLeaf != oldLeaf) {
                    for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                        notifyPublicListeners(
                            dynamicProperty(anyConfig, key).listeners(),
                            oldLeaf,
                            newLeaf,
                            storageRevision,
                            futures,
                            ConfigurationListener::onUpdate
                        );
                    }

                    notifyPublicListeners(
                        dynamicProperty(cfgNode, key).listeners(),
                        oldLeaf,
                        newLeaf,
                        storageRevision,
                        futures,
                        ConfigurationListener::onUpdate
                    );
                }

                return null;
            }

            /** {@inheritDoc} */
            @Override public Void visitInnerNode(String key, InnerNode oldNode) {
                InnerNode newNode = newInnerNode.traverseChild(key, innerNodeVisitor(), true);

                notifyListeners(
                    oldNode,
                    newNode,
                    dynamicConfig(cfgNode, key),
                    storageRevision,
                    futures,
                    view(anyConfigs, cfg -> dynamicConfig(cfg, key))
                );

                return null;
            }

            /** {@inheritDoc} */
            @Override public <N extends InnerNode> Void visitNamedListNode(String key, NamedListNode<N> oldNamedList) {
                var newNamedList = (NamedListNode<InnerNode>)newInnerNode.traverseChild(key, namedListNodeVisitor(), true);

                if (newNamedList != oldNamedList) {
                    for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                        notifyPublicListeners(
                            namedDynamicConfig(anyConfig, key).listeners(),
                            (NamedListView<InnerNode>)oldNamedList,
                            newNamedList,
                            storageRevision,
                            futures,
                            ConfigurationListener::onUpdate
                        );
                    }

                    notifyPublicListeners(
                        namedDynamicConfig(cfgNode, key).listeners(),
                        (NamedListView<InnerNode>)oldNamedList,
                        newNamedList,
                        storageRevision,
                        futures,
                        ConfigurationListener::onUpdate
                    );

                    // This is optimization, we could use "NamedListConfiguration#get" directly, but we don't want to.
                    List<String> oldNames = oldNamedList.namedListKeys();
                    List<String> newNames = newNamedList.namedListKeys();

                    NamedListConfiguration<?, InnerNode, ?> namedListCfg = namedDynamicConfig(cfgNode, key);

                    Map<String, ConfigurationProperty<?, ?>> namedListCfgMembers = namedListCfg.touchMembers();

                    Set<String> created = new HashSet<>(newNames);
                    created.removeAll(oldNames);

                    Set<String> deleted = new HashSet<>(oldNames);
                    deleted.removeAll(newNames);

                    Map<String, String> renamed = new HashMap<>();
                    if (!created.isEmpty() && !deleted.isEmpty()) {
                        Map<String, String> createdIds = new HashMap<>();

                        for (String createdKey : created)
                            createdIds.put(newNamedList.internalId(createdKey), createdKey);

                        // Avoiding ConcurrentModificationException.
                        for (String deletedKey : Set.copyOf(deleted)) {
                            String internalId = oldNamedList.internalId(deletedKey);

                            String maybeRenamedKey = createdIds.get(internalId);

                            if (maybeRenamedKey == null)
                                continue;

                            deleted.remove(deletedKey);
                            created.remove(maybeRenamedKey);
                            renamed.put(deletedKey, maybeRenamedKey);
                        }
                    }

                    for (String name : created) {
                        for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                            notifyPublicListeners(
                                namedDynamicConfig(anyConfig, key).extendedListeners(),
                                null,
                                newNamedList.get(name),
                                storageRevision,
                                futures,
                                ConfigurationNamedListListener::onCreate
                            );
                        }

                        notifyPublicListeners(
                            namedDynamicConfig(cfgNode, key).extendedListeners(),
                            null,
                            newNamedList.get(name),
                            storageRevision,
                            futures,
                            ConfigurationNamedListListener::onCreate
                        );

                        touch((DynamicConfiguration<?, ?>)namedListCfg.members().get(name));
                    }

                    for (String name : deleted) {
                        for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                            notifyPublicListeners(
                                namedDynamicConfig(anyConfig, key).extendedListeners(),
                                oldNamedList.get(name),
                                null,
                                storageRevision,
                                futures,
                                ConfigurationNamedListListener::onDelete
                            );
                        }

                        notifyPublicListeners(
                            namedDynamicConfig(cfgNode, key).extendedListeners(),
                            oldNamedList.get(name),
                            null,
                            storageRevision,
                            futures,
                            ConfigurationNamedListListener::onDelete
                        );

                        // Notification for deleted configuration.

                        for (var anyConfig : view(anyConfigs, cfg -> any(namedDynamicConfig(cfg, key)))) {
                            notifyPublicListeners(
                                anyConfig.listeners(),
                                oldNamedList.get(name),
                                null,
                                storageRevision,
                                futures,
                                ConfigurationListener::onUpdate
                            );
                        }

                        notifyPublicListeners(
                            ((DynamicConfiguration<InnerNode, ?>)namedListCfgMembers.get(name)).listeners(),
                            oldNamedList.get(name),
                            null,
                            storageRevision,
                            futures,
                            ConfigurationListener::onUpdate
                        );
                    }

                    for (Map.Entry<String, String> entry : renamed.entrySet()) {
                        for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                            notifyPublicListeners(
                                namedDynamicConfig(anyConfig, key).extendedListeners(),
                                oldNamedList.get(entry.getKey()),
                                newNamedList.get(entry.getValue()),
                                storageRevision,
                                futures,
                                (listener, evt) -> listener.onRename(entry.getKey(), entry.getValue(), evt)
                            );
                        }

                        notifyPublicListeners(
                            namedDynamicConfig(cfgNode, key).extendedListeners(),
                            oldNamedList.get(entry.getKey()),
                            newNamedList.get(entry.getValue()),
                            storageRevision,
                            futures,
                            (listener, evt) -> listener.onRename(entry.getKey(), entry.getValue(), evt)
                        );
                    }

                    for (String name : newNames) {
                        if (!oldNames.contains(name))
                            continue;

                        for (DynamicConfiguration<InnerNode, ?> anyConfig : anyConfigs) {
                            notifyPublicListeners(
                                namedDynamicConfig(anyConfig, key).extendedListeners(),
                                oldNamedList.get(name),
                                newNamedList.get(name),
                                storageRevision,
                                futures,
                                ConfigurationListener::onUpdate
                            );
                        }

                        notifyPublicListeners(
                            namedDynamicConfig(cfgNode, key).extendedListeners(),
                            oldNamedList.get(name),
                            newNamedList.get(name),
                            storageRevision,
                            futures,
                            ConfigurationListener::onUpdate
                        );

                        notifyListeners(
                            oldNamedList.get(name),
                            newNamedList.get(name),
                            (DynamicConfiguration<InnerNode, ?>)namedListCfgMembers.get(name),
                            storageRevision,
                            futures,
                            concat(view(anyConfigs, cfg -> any(namedDynamicConfig(cfg, key))), List.of(any(namedListCfg)))
                        );
                    }
                }

                return null;
            }
        }, true);
    }

    /**
     * Invoke {@link ConfigurationListener#onUpdate(ConfigurationNotificationEvent)} on all passed listeners and put
     * results in {@code futures}. Not recursively.
     *
     * @param listeners List o listeners.
     * @param oldVal Old configuration value.
     * @param newVal New configuration value.
     * @param storageRevision Storage revision.
     * @param futures Write-only list of futures.
     * @param updater Update closure to be invoked on the listener instance.
     * @param <V> Type of the node.
     * @param <L> Type of the configuration listener.
     */
    private static <V, L extends ConfigurationListener<V>> void notifyPublicListeners(
        List<? extends L> listeners,
        V oldVal,
        V newVal,
        long storageRevision,
        List<CompletableFuture<?>> futures,
        BiFunction<L, ConfigurationNotificationEvent<V>, CompletableFuture<?>> updater
    ) {
        if (!listeners.isEmpty()) {
            ConfigurationNotificationEvent<V> evt = new ConfigurationNotificationEventImpl<>(
                oldVal,
                newVal,
                storageRevision
            );

            for (L listener : listeners) {
                try {
                    CompletableFuture<?> future = updater.apply(listener, evt);

                    assert future != null : updater;

                    if (future.isCompletedExceptionally() || future.isCancelled() || !future.isDone())
                        futures.add(future);
                }
                catch (Throwable t) {
                    futures.add(CompletableFuture.failedFuture(t));
                }
            }
        }
    }

    /**
     * Ensures that dynamic configuration tree is up to date and further notifications on it will be invoked correctly.
     *
     * @param cfg Dynamic configuration node instance.
     */
    public static void touch(DynamicConfiguration<?, ?> cfg) {
        cfg.touchMembers();

        for (ConfigurationProperty<?, ?> value : cfg.members().values()) {
            if (value instanceof DynamicConfiguration)
                touch((DynamicConfiguration<?, ?>)value);
        }
    }

    /**
     * Get the dynamic property of the leaf.
     *
     * @param dynamicConfig Dynamic configuration.
     * @param nodeName Name of the child node.
     * @return Dynamic property of a leaf.
     */
    private static DynamicProperty<Serializable> dynamicProperty(
        DynamicConfiguration<InnerNode, ?> dynamicConfig,
        String nodeName
    ) {
        return (DynamicProperty<Serializable>)dynamicConfig.members().get(nodeName);
    }

    /**
     * Get the dynamic configuration of the child node.
     *
     * @param dynamicConfig Dynamic configuration.
     * @param nodeName Name of the child node.
     * @return Dynamic configuration of the child node.
     */
    private static DynamicConfiguration<InnerNode, ?> dynamicConfig(
        DynamicConfiguration<InnerNode, ?> dynamicConfig,
        String nodeName
    ) {
        return (DynamicConfiguration<InnerNode, ?>)dynamicConfig.members().get(nodeName);
    }

    /**
     * Get the named dynamic configuration of the child node.
     *
     * @param dynamicConfig Dynamic configuration.
     * @param nodeName Name of the child node.
     * @return Named dynamic configuration of the child node.
     */
    private static NamedListConfiguration<?, InnerNode, ?> namedDynamicConfig(
        DynamicConfiguration<InnerNode, ?> dynamicConfig,
        String nodeName
    ) {
        return (NamedListConfiguration<?, InnerNode, ?>)dynamicConfig.members().get(nodeName);
    }

    /**
     * Get the dynamic configuration of the {@link NamedListConfiguration#any any} node.
     *
     * @param namedConfig Dynamic configuration.
     * @return Dynamic configuration of the "any" node.
     */
    private static DynamicConfiguration<InnerNode, ?> any(NamedListConfiguration<?, InnerNode, ?> namedConfig) {
        return (DynamicConfiguration<InnerNode, ?>)namedConfig.any();
    }

    /**
     * Get the dynamic configuration of the child node.
     *
     * @param namedConfig Named dynamic configuration.
     * @param nodeName Name of the child node.
     * @return Dynamic configuration of the child node.
     */
    private static DynamicConfiguration<InnerNode, ?> dynamicConfig(
        NamedListConfiguration<?, InnerNode, ?> namedConfig,
        String nodeName
    ) {
        return (DynamicConfiguration<InnerNode, ?>)namedConfig.members().get(nodeName);
    }
}
