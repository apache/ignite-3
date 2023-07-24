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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotifier.notifyListeners;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.checkConfigurationType;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.innerNodeVisitor;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.ConfigurationChanger.ConfigurationUpdateListener;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.util.KeyNotFoundException;
import org.apache.ignite.internal.configuration.util.NodeValue;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
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

    /**
     * Registers default validator implementation to the validators map.
     *
     * @param validators     Validators map.
     * @param annotatopnType Annotation type instance for the validator.
     * @param validator      Validator instance.
     * @param <A>            Annotation type.
     */
    private static <A extends Annotation> void addDefaultValidator(
            Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators,
            Class<A> annotatopnType,
            Validator<A, ?> validator
    ) {
        validators.computeIfAbsent(annotatopnType, a -> new HashSet<>(1)).add(validator);
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        changer.start();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        changer.stop();
    }

    /**
     * Returns a future that resolves after the defaults are persisted to the storage.
     */
    public CompletableFuture<Void> onDefaultsPersisted() {
        return changer.onDefaultsPersisted();
    }

    /**
     * Gets the public configuration tree.
     *
     * @param rootKey Root key.
     * @param <V>     View type.
     * @param <C>     Change type.
     * @param <T>     Configuration tree type.
     * @return Public configuration tree.
     */
    public <V, C, T extends ConfigurationTree<V, C>> T getConfiguration(RootKey<T, V> rootKey) {
        return (T) configs.get(rootKey.key());
    }

    /**
     * Convert configuration subtree into a user-defined representation.
     *
     * @param path    Path to configuration subtree. Can be empty, can't be {@code null}.
     * @param visitor Visitor that will be applied to the subtree and build the representation.
     * @param <T>     Type of the representation.
     * @return User-defined representation constructed by {@code visitor}.
     * @throws IllegalArgumentException If {@code path} is not found in current configuration.
     */
    public <T> T represent(List<String> path, ConfigurationVisitor<T> visitor) throws IllegalArgumentException {
        SuperRoot superRoot = changer.superRoot();

        NodeValue<?> node;
        try {
            node = ConfigurationUtil.find(path, superRoot, false);
        } catch (KeyNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage());
        }

        Object value = node.value();
        if (value instanceof TraversableTreeNode) {
            return ((TraversableTreeNode) value).accept(node.field(), null, visitor);
        }

        assert value == null || value instanceof Serializable;

        return visitor.visitLeafNode(node.field(), null, (Serializable) value);
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

                change.accept(new SuperRootChange() {
                    @Override
                    public <V> V viewRoot(RootKey<? extends ConfigurationTree<V, ?>, V> rootKey) {
                        return Objects.requireNonNull(superRoot.getRoot(rootKey)).specificNode();
                    }

                    @Override
                    public <C> C changeRoot(RootKey<? extends ConfigurationTree<?, C>, ?> rootKey) {
                        // "construct" does a field copying, which is what we need before mutating it.
                        superRoot.construct(rootKey.key(), ConfigurationUtil.EMPTY_CFG_SRC, true);

                        // "rootView" is not re-used here because of return type incompatibility, although code is the same.
                        return Objects.requireNonNull(superRoot.getRoot(rootKey)).specificNode();
                    }
                });
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
                    return completedFuture(null);
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
