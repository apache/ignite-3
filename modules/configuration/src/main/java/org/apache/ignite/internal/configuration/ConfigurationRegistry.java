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

import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotifier.notifyListeners;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.checkConfigurationType;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.innerNodeVisitor;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.ConfigurationChanger.ConfigurationUpdateListener;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration registry.
 */
public class ConfigurationRegistry implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ConfigurationRegistry.class);

    /** Generated configuration implementations. Mapping: {@link RootKey#key} -> configuration implementation. */
    private final Map<String, DynamicConfiguration<?, ?>> configs = new HashMap<>();

    /** Configuration change handler. */
    private final ConfigurationChanger changer;

    /**
     * Constructor.
     *
     * @param rootKeys                    Configuration root keys.
     * @param storage                     Configuration storage.
     * @param generator                   Configuration tree generator.
     * @param configurationValidator      Configuration validator.
     * @throws IllegalArgumentException If the configuration type of the root keys is not equal to the storage type, or if the schema or its
     *                                  extensions are not valid.
     */
    public ConfigurationRegistry(
            Collection<RootKey<?, ?>> rootKeys,
            ConfigurationStorage storage,
            ConfigurationTreeGenerator generator,
            ConfigurationValidator configurationValidator
    ) {
        checkConfigurationType(rootKeys, storage);

        changer = new ConfigurationChanger(notificationUpdateListener(), rootKeys, storage, configurationValidator) {
            @Override
            public InnerNode createRootNode(RootKey<?, ?> rootKey) {
                return generator.instantiateNode(rootKey.schemaClass());
            }
        };

        rootKeys.forEach(rootKey -> {
            DynamicConfiguration<?, ?> cfg = generator.instantiateCfg(rootKey, changer);

            configs.put(rootKey.key(), cfg);
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        changer.start();

        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        changer.stop();

        return nullCompletedFuture();
    }

    /**
     * Returns a future that resolves after the defaults are persisted to the storage.
     */
    public CompletableFuture<Void> onDefaultsPersisted() {
        return changer.onDefaultsPersisted();
    }

    /**
     * Initializes the configuration with the given source. This method should be used only for the initial setup of the configuration. The
     * configuration is initialized with the provided source only if the storage is empty, and it is saved along with the defaults. This
     * method must be called before {@link #startAsync()}.
     *
     * @param configurationSource the configuration source to initialize with.
     */
    public void initializeConfigurationWith(ConfigurationSource configurationSource) {
        changer.initializeConfigurationWith(configurationSource);
    }

    /**
     * Gets the public configuration tree.
     *
     * @param rootKey Root key.
     * @param <V> View type.
     * @param <C> Change type.
     * @param <T> Configuration tree type.
     * @return Public configuration tree.
     */
    public <V, C, T extends ConfigurationTree<? super V, C>> T getConfiguration(RootKey<T, V> rootKey) {
        return (T) configs.get(rootKey.key());
    }

    /**
     * Returns a copy of the configuration root.
     *
     * @return Copy of the configuration root.
     */
    public SuperRoot superRoot() {
        return changer.superRoot().copy();
    }

    /**
     * Change configuration.
     *
     * @param changesSrc Configuration source to create patch from it.
     * @return Future that is completed on change completion.
     */
    public CompletableFuture<Void> change(ConfigurationSource changesSrc) {
        return changer.change(changesSrc);
    }

    /**
     * Change configuration. Gives the possibility to atomically update several root trees.
     *
     * @param change Closure that would consume a mutable super root instance.
     * @return Future that is completed on change completion.
     */
    public CompletableFuture<Void> change(Consumer<SuperRootChange> change) {
        return change(new ConfigurationSource() {
            @Override
            public void descend(ConstructableTreeNode node) {
                assert node instanceof SuperRoot : "Descending always starts with super root: " + node;

                SuperRoot superRoot = (SuperRoot) node;

                change.accept(new SuperRootChangeImpl(superRoot));
            }
        });
    }

    private ConfigurationUpdateListener notificationUpdateListener() {
        return new ConfigurationUpdateListener() {
            @Override
            public CompletableFuture<Void> onConfigurationUpdated(
                    @Nullable SuperRoot oldSuperRoot, SuperRoot newSuperRoot, long storageRevision, long notificationNumber
            ) {
                var futures = new ArrayList<CompletableFuture<?>>();

                newSuperRoot.traverseChildren(new ConfigurationVisitor<Void>() {
                    @Override
                    public Void visitInnerNode(Field field, String key, InnerNode newRoot) {
                        DynamicConfiguration<InnerNode, ?> config = (DynamicConfiguration<InnerNode, ?>) configs.get(key);

                        assert config != null : key;

                        InnerNode oldRoot;

                        if (oldSuperRoot != null) {
                            oldRoot = oldSuperRoot.traverseChild(key, innerNodeVisitor(), true);

                            assert oldRoot != null : key;
                        } else {
                            oldRoot = null;
                        }

                        futures.addAll(notifyListeners(oldRoot, newRoot, config, storageRevision, notificationNumber));

                        return null;
                    }
                }, true);

                return combineFutures(futures);
            }

            private CompletableFuture<Void> combineFutures(Collection<CompletableFuture<?>> futures) {
                if (futures.isEmpty()) {
                    return nullCompletedFuture();
                }

                CompletableFuture<?>[] resultFutures = futures.stream()
                        // Map futures is only for logging errors.
                        .map(fut -> fut.whenComplete((res, throwable) -> {
                            if (throwable != null) {
                                LOG.info("Failed to notify configuration listener", throwable);
                            }
                        }))
                        .toArray(CompletableFuture[]::new);

                return CompletableFuture.allOf(resultFutures);
            }
        };
    }

    /**
     * Notifies all listeners of the current configuration.
     *
     * <p>{@link ConfigurationListener#onUpdate} and {@link ConfigurationNamedListListener#onCreate} will be called and the value will
     * only be in {@link ConfigurationNotificationEvent#newValue}.
     *
     * @return Future that must signify when processing is completed.
     */
    public CompletableFuture<Void> notifyCurrentConfigurationListeners() {
        return changer.notifyCurrentConfigurationListeners();
    }

    /**
     * Returns the count of configuration listener notifications.
     *
     * <p>Monotonically increasing value that should be incremented each time an attempt is made to notify all listeners of the
     * configuration. Allows to guarantee that new listeners will be called only on the next notification of all configuration listeners.
     */
    public long notificationCount() {
        return changer.notificationCount();
    }
}
