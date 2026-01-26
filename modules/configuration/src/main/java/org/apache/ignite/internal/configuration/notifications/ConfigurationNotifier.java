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

import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.any;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.dynamicConfig;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.dynamicProperty;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.extendedListeners;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.listeners;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.mergeAnyConfigs;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationUtils.namedDynamicConfig;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.innerNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.leafNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.mapIterable;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.namedListNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.touch;
import static org.apache.ignite.internal.util.CollectionUtils.concat;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.jetbrains.annotations.Nullable;

/**
 * Class for notifying configuration listeners.
 */
public class ConfigurationNotifier {
    /**
     * Recursive notification of all configuration listeners.
     *
     * <p>NOTE: If {@code oldInnerNode == null}, then {@link ConfigurationListener#onUpdate} and
     * {@link ConfigurationNamedListListener#onCreate} will be called and the value will only be in
     * {@link ConfigurationNotificationEvent#newValue}.
     *
     * @param oldInnerNode Old configuration values root.
     * @param newInnerNode New configuration values root.
     * @param config Public configuration tree node corresponding to the current inner nodes.
     * @param storageRevision Storage revision.
     * @param notificationNumber Notification number.
     * @return Collected configuration listener futures.
     * @see ConfigurationListener
     * @see ConfigurationNamedListListener
     */
    public static Collection<CompletableFuture<?>> notifyListeners(
            @Nullable InnerNode oldInnerNode,
            InnerNode newInnerNode,
            DynamicConfiguration<InnerNode, ?> config,
            long storageRevision,
            long notificationNumber
    ) {
        if (oldInnerNode == newInnerNode) {
            return List.of();
        }

        ConfigurationNotificationContext ctx = new ConfigurationNotificationContext(storageRevision, notificationNumber);

        ctx.addContainer(oldInnerNode, newInnerNode, null, null);

        if (oldInnerNode == null) {
            notifyListeners(newInnerNode, config, List.of(), ctx);
        } else {
            notifyListeners(oldInnerNode, newInnerNode, config, List.of(), ctx);
        }

        ctx.removeContainer();

        return ctx.futures;
    }

    private static void notifyListeners(
            @Nullable InnerNode oldInnerNode,
            InnerNode newInnerNode,
            DynamicConfiguration<InnerNode, ?> config,
            Iterable<DynamicConfiguration<InnerNode, ?>> anyConfigs,
            ConfigurationNotificationContext ctx
    ) {
        assert !(config instanceof NamedListConfiguration);

        if (oldInnerNode == null || newInnerNode == oldInnerNode) {
            return;
        }

        notifyPublicListeners(
                listeners(config, ctx.notificationNum),
                concat(mapIterable(anyConfigs, anyCfg -> listeners(anyCfg, ctx.notificationNum))),
                oldInnerNode.specificNode(),
                newInnerNode.specificNode(),
                ctx,
                ConfigurationListener::onUpdate
        );

        // Polymorphic configuration type has changed.
        // At the moment, we do not separate common fields from fields of a specific polymorphic configuration,
        // so this may cause errors in the logic below, perhaps we will fix it later.
        // TODO: https://issues.apache.org/jira/browse/IGNITE-15916
        if (oldInnerNode.schemaType() != newInnerNode.schemaType()) {
            return;
        }

        oldInnerNode.traverseChildren(new ConfigurationVisitor<Void>() {
            /** {@inheritDoc} */
            @Override
            public Void visitLeafNode(Field field, String key, Serializable oldLeaf) {
                Serializable newLeaf = newInnerNode.traverseChild(key, leafNodeVisitor(), true);

                if (newLeaf != oldLeaf) {
                    notifyPublicListeners(
                            listeners(dynamicProperty(config, key), ctx.notificationNum),
                            concat(mapIterable(anyConfigs, anyCfg -> listeners(dynamicProperty(anyCfg, key), ctx.notificationNum))),
                            oldLeaf,
                            newLeaf,
                            ctx,
                            ConfigurationListener::onUpdate
                    );
                }

                return null;
            }

            /** {@inheritDoc} */
            @Override
            public Void visitInnerNode(Field field, String key, InnerNode oldNode) {
                InnerNode newNode = newInnerNode.traverseChild(key, innerNodeVisitor(), true);

                DynamicConfiguration<InnerNode, ?> newConfig = dynamicConfig(config, key);

                ctx.addContainer(oldNode, newNode, null, null);

                notifyListeners(
                        oldNode,
                        newNode,
                        newConfig,
                        mapIterable(anyConfigs, anyCfg -> dynamicConfig(anyCfg, key)),
                        ctx
                );

                ctx.removeContainer();

                return null;
            }

            /** {@inheritDoc} */
            @Override
            public Void visitNamedListNode(Field field, String key, NamedListNode<?> oldNamedList) {
                NamedListNode<InnerNode> newNamedList =
                        (NamedListNode<InnerNode>) newInnerNode.traverseChild(key, namedListNodeVisitor(), true);

                if (newNamedList != oldNamedList) {
                    notifyPublicListeners(
                            listeners(namedDynamicConfig(config, key), ctx.notificationNum),
                            concat(mapIterable(anyConfigs, anyCfg -> listeners(namedDynamicConfig(anyCfg, key), ctx.notificationNum))),
                            oldNamedList,
                            newNamedList,
                            ctx,
                            ConfigurationListener::onUpdate
                    );

                    NamedListChanges namedListChanges = NamedListChanges.of(oldNamedList, newNamedList);

                    NamedListConfiguration<?, InnerNode, ?> namedListCfg = namedDynamicConfig(config, key);

                    Map<String, ConfigurationProperty<?>> namedListCfgMembers = namedListCfg.touchMembers();

                    // Lazy initialization.
                    Iterable<DynamicConfiguration<InnerNode, ?>> newAnyConfigs = null;

                    for (String name : namedListChanges.created) {
                        DynamicConfiguration<InnerNode, ?> newNodeCfg =
                                (DynamicConfiguration<InnerNode, ?>) namedListCfg.members().get(name);

                        touch(newNodeCfg);

                        InnerNode newVal = newNamedList.getInnerNode(name);

                        ctx.addContainer(null, newVal, null, name);

                        notifyPublicListeners(
                                extendedListeners(namedDynamicConfig(config, key), ctx.notificationNum),
                                concat(mapIterable(
                                        anyConfigs,
                                        anyCfg -> extendedListeners(namedDynamicConfig(anyCfg, key), ctx.notificationNum)
                                )),
                                null,
                                newVal.specificNode(),
                                ctx,
                                ConfigurationNamedListListener::onCreate
                        );

                        if (newAnyConfigs == null) {
                            newAnyConfigs = mergeAnyConfigs(
                                    mapIterable(anyConfigs, anyCfg -> any(namedDynamicConfig(anyCfg, key))),
                                    any(namedListCfg)
                            );
                        }

                        notifyListeners(
                                newVal,
                                newNodeCfg,
                                newAnyConfigs,
                                ctx
                        );

                        ctx.removeContainer();
                    }

                    for (String name : namedListChanges.deleted) {
                        DynamicConfiguration<InnerNode, ?> delNodeCfg =
                                (DynamicConfiguration<InnerNode, ?>) namedListCfgMembers.get(name);

                        InnerNode oldVal = oldNamedList.getInnerNode(name);

                        ctx.addContainer(oldVal, null, name, null);

                        notifyPublicListeners(
                                extendedListeners(namedDynamicConfig(config, key), ctx.notificationNum),
                                concat(mapIterable(
                                        anyConfigs,
                                        anyCfg -> extendedListeners(namedDynamicConfig(anyCfg, key), ctx.notificationNum)
                                )),
                                oldVal.specificNode(),
                                null,
                                ctx,
                                ConfigurationNamedListListener::onDelete
                        );

                        // Notification for deleted configuration.

                        notifyPublicListeners(
                                listeners(delNodeCfg, ctx.notificationNum),
                                concat(mapIterable(
                                        anyConfigs,
                                        anyCfg -> listeners(any(namedDynamicConfig(anyCfg, key)), ctx.notificationNum)
                                )),
                                oldVal.specificNode(),
                                null,
                                ctx,
                                ConfigurationListener::onUpdate
                        );

                        ctx.removeContainer();
                    }

                    for (Map.Entry<String, String> entry : namedListChanges.renamed.entrySet()) {
                        InnerNode oldVal = oldNamedList.getInnerNode(entry.getKey());
                        InnerNode newVal = newNamedList.getInnerNode(entry.getValue());

                        ctx.addContainer(oldVal, newVal, entry.getKey(), entry.getValue());

                        notifyPublicListeners(
                                extendedListeners(namedDynamicConfig(config, key), ctx.notificationNum),
                                concat(mapIterable(
                                        anyConfigs,
                                        anyCfg -> extendedListeners(namedDynamicConfig(anyCfg, key), ctx.notificationNum)
                                )),
                                oldVal.specificNode(),
                                newVal.specificNode(),
                                ctx,
                                (listener, event) -> listener.onRename(event)
                        );

                        ctx.removeContainer();
                    }

                    for (String name : namedListChanges.updated) {
                        InnerNode oldVal = oldNamedList.getInnerNode(name);
                        InnerNode newVal = newNamedList.getInnerNode(name);

                        if (oldVal == newVal) {
                            continue;
                        }

                        DynamicConfiguration<InnerNode, ?> updNodeCfg =
                                (DynamicConfiguration<InnerNode, ?>) namedListCfgMembers.get(name);

                        ctx.addContainer(oldVal, newVal, name, name);

                        notifyPublicListeners(
                                extendedListeners(namedDynamicConfig(config, key), ctx.notificationNum),
                                concat(mapIterable(
                                        anyConfigs,
                                        anyCfg -> extendedListeners(namedDynamicConfig(anyCfg, key), ctx.notificationNum)
                                )),
                                oldVal.specificNode(),
                                newVal.specificNode(),
                                ctx,
                                ConfigurationNamedListListener::onUpdate
                        );

                        if (newAnyConfigs == null) {
                            newAnyConfigs = mergeAnyConfigs(
                                    mapIterable(anyConfigs, anyCfg -> any(namedDynamicConfig(anyCfg, key))),
                                    any(namedListCfg)
                            );
                        }

                        notifyListeners(
                                oldVal,
                                newVal,
                                updNodeCfg,
                                newAnyConfigs,
                                ctx
                        );

                        ctx.removeContainer();
                    }
                }

                return null;
            }
        }, true);
    }

    /**
     * Recursive notification of all configuration listeners.
     *
     * <p>NOTE: Only {@link ConfigurationListener#onUpdate} and {@link ConfigurationNamedListListener#onCreate} will be called.
     *
     * <p>NOTE: Value will only be in {@link ConfigurationNotificationEvent#newValue}.
     */
    private static void notifyListeners(
            InnerNode innerNode,
            DynamicConfiguration<InnerNode, ?> config,
            Iterable<DynamicConfiguration<InnerNode, ?>> anyConfigs,
            ConfigurationNotificationContext ctx
    ) {
        assert !(config instanceof NamedListConfiguration);

        notifyPublicListeners(
                listeners(config, ctx.notificationNum),
                concat(mapIterable(anyConfigs, anyCfg -> listeners(anyCfg, ctx.notificationNum))),
                null,
                innerNode.specificNode(),
                ctx,
                ConfigurationListener::onUpdate
        );

        innerNode.traverseChildren(new ConfigurationVisitor<Void>() {
            /** {@inheritDoc} */
            @Override
            public Void visitLeafNode(Field field, String key, Serializable leaf) {
                notifyPublicListeners(
                        listeners(dynamicProperty(config, key), ctx.notificationNum),
                        concat(mapIterable(
                                anyConfigs,
                                anyCfg -> listeners(dynamicProperty(anyCfg, key), ctx.notificationNum)
                        )),
                        null,
                        leaf,
                        ctx,
                        ConfigurationListener::onUpdate
                );

                return null;
            }

            /** {@inheritDoc} */
            @Override
            public Void visitInnerNode(Field field, String key, InnerNode nestedInnerNode) {
                DynamicConfiguration<InnerNode, ?> nestedNodeConfig = dynamicConfig(config, key);

                ctx.addContainer(null, nestedInnerNode, null, null);

                notifyListeners(
                        nestedInnerNode,
                        nestedNodeConfig,
                        mapIterable(anyConfigs, anyCfg -> dynamicConfig(anyCfg, key)),
                        ctx
                );

                ctx.removeContainer();

                return null;
            }

            /** {@inheritDoc} */
            @Override
            public Void visitNamedListNode(Field field, String key, NamedListNode<?> newNamedList) {
                notifyPublicListeners(
                        listeners(namedDynamicConfig(config, key), ctx.notificationNum),
                        concat(mapIterable(
                                anyConfigs,
                                anyCfg -> listeners(namedDynamicConfig(anyCfg, key), ctx.notificationNum)
                        )),
                        null,
                        newNamedList,
                        ctx,
                        ConfigurationListener::onUpdate
                );

                // Created only.

                // Lazy initialization.
                Iterable<DynamicConfiguration<InnerNode, ?>> newAnyConfigs = null;

                NamedListConfiguration<?, InnerNode, ?> namedListCfg = namedDynamicConfig(config, key);

                // Initialize named list configuration.
                namedListCfg.touchMembers();

                for (String name : newNamedList.namedListKeys()) {
                    DynamicConfiguration<InnerNode, ?> namedNodeConfig =
                            (DynamicConfiguration<InnerNode, ?>) namedListCfg.getConfig(name);

                    InnerNode namedInnerNode = newNamedList.getInnerNode(name);

                    ctx.addContainer(null, namedInnerNode, null, name);

                    notifyPublicListeners(
                            extendedListeners(namedDynamicConfig(config, key), ctx.notificationNum),
                            concat(mapIterable(
                                    anyConfigs,
                                    anyCfg -> extendedListeners(namedDynamicConfig(anyCfg, key), ctx.notificationNum)
                            )),
                            null,
                            namedInnerNode.specificNode(),
                            ctx,
                            ConfigurationNamedListListener::onCreate
                    );

                    if (newAnyConfigs == null) {
                        newAnyConfigs = mergeAnyConfigs(
                                mapIterable(anyConfigs, anyCfg -> any(namedDynamicConfig(anyCfg, key))),
                                any(namedDynamicConfig(config, key))
                        );
                    }

                    notifyListeners(
                            namedInnerNode,
                            namedNodeConfig,
                            newAnyConfigs,
                            ctx
                    );

                    ctx.removeContainer();
                }

                return null;
            }
        }, true);
    }

    private static <L extends ConfigurationListener<?>> void notifyPublicListeners(
            Iterator<? extends L> configListeners,
            Iterator<? extends L> anyListeners,
            @Nullable Object oldValue,
            @Nullable Object newValue,
            ConfigurationNotificationContext notificationCtx,
            BiFunction<L, ConfigurationNotificationEvent, CompletableFuture<?>> invokeListener
    ) {
        // Lazy set.
        ConfigurationNotificationEvent<?> event = null;

        for (Iterator<? extends L> it = concat(anyListeners, configListeners); it.hasNext(); ) {
            if (event == null) {
                event = notificationCtx.createEvent(oldValue, newValue);
            }

            try {
                CompletableFuture<?> future = invokeListener.apply(it.next(), event);

                assert future != null : invokeListener;

                if (future.isCompletedExceptionally() || future.isCancelled() || !future.isDone()) {
                    notificationCtx.futures.add(future);
                }
            } catch (Throwable t) {
                notificationCtx.futures.add(CompletableFuture.failedFuture(t));
            }
        }
    }
}
