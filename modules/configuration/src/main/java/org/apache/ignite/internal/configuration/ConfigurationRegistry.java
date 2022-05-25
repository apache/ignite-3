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

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotifier.notifyListeners;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.checkConfigurationType;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.collectSchemas;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.innerNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.internalSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicInstanceId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.schemaFields;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.touch;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CollectionUtils.viewReadOnly;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.validation.ExceptKeys;
import org.apache.ignite.configuration.validation.Immutable;
import org.apache.ignite.configuration.validation.OneOf;
import org.apache.ignite.configuration.validation.PowerOfTwo;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListener;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListenerHolder;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.util.KeyNotFoundException;
import org.apache.ignite.internal.configuration.validation.ExceptKeysValidator;
import org.apache.ignite.internal.configuration.validation.ImmutableValidator;
import org.apache.ignite.internal.configuration.validation.OneOfValidator;
import org.apache.ignite.internal.configuration.validation.PowerOfTwoValidator;
import org.apache.ignite.internal.configuration.validation.RangeValidator;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration registry.
 */
public class ConfigurationRegistry implements IgniteComponent, ConfigurationStorageRevisionListenerHolder {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ConfigurationRegistry.class);

    /** Generated configuration implementations. Mapping: {@link RootKey#key} -> configuration implementation. */
    private final Map<String, DynamicConfiguration<?, ?>> configs = new HashMap<>();

    /** Root keys. */
    private final Collection<RootKey<?, ?>> rootKeys;

    /** Configuration change handler. */
    private final ConfigurationChanger changer;

    /** Configuration generator. */
    private final ConfigurationAsmGenerator cgen = new ConfigurationAsmGenerator();

    /** Configuration storage revision change listeners. */
    private final ConfigurationListenerHolder<ConfigurationStorageRevisionListener> storageRevisionListeners =
            new ConfigurationListenerHolder<>();

    /**
     * Constructor.
     *
     * @param rootKeys                    Configuration root keys.
     * @param validators                  Validators.
     * @param storage                     Configuration storage.
     * @param internalSchemaExtensions    Internal extensions ({@link InternalConfiguration}) of configuration schemas ({@link
     *                                    ConfigurationRoot} and {@link Config}).
     * @param polymorphicSchemaExtensions Polymorphic extensions ({@link PolymorphicConfigInstance}) of configuration schemas.
     * @throws IllegalArgumentException If the configuration type of the root keys is not equal to the storage type, or if the schema or its
     *                                  extensions are not valid.
     */
    public ConfigurationRegistry(
            Collection<RootKey<?, ?>> rootKeys,
            Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> validators,
            ConfigurationStorage storage,
            Collection<Class<?>> internalSchemaExtensions,
            Collection<Class<?>> polymorphicSchemaExtensions
    ) {
        checkConfigurationType(rootKeys, storage);

        Set<Class<?>> allSchemas = collectAllSchemas(rootKeys, internalSchemaExtensions, polymorphicSchemaExtensions);

        final Map<Class<?>, Set<Class<?>>> internalExtensions = internalExtensionsWithCheck(allSchemas, internalSchemaExtensions);

        final Map<Class<?>, Set<Class<?>>> polymorphicExtensions = polymorphicExtensionsWithCheck(allSchemas, polymorphicSchemaExtensions);

        this.rootKeys = rootKeys;

        Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators0 = new HashMap<>(validators);

        addDefaultValidator(validators0, Immutable.class, new ImmutableValidator());
        addDefaultValidator(validators0, OneOf.class, new OneOfValidator());
        addDefaultValidator(validators0, ExceptKeys.class, new ExceptKeysValidator());
        addDefaultValidator(validators0, PowerOfTwo.class, new PowerOfTwoValidator());
        addDefaultValidator(validators0, Range.class, new RangeValidator());

        changer = new ConfigurationChanger(this::notificator, rootKeys, validators0, storage) {
            /** {@inheritDoc} */
            @Override
            public InnerNode createRootNode(RootKey<?, ?> rootKey) {
                return cgen.instantiateNode(rootKey.schemaClass());
            }
        };

        rootKeys.forEach(rootKey -> {
            cgen.compileRootSchema(rootKey.schemaClass(), internalExtensions, polymorphicExtensions);

            DynamicConfiguration<?, ?> cfg = cgen.instantiateCfg(rootKey, changer);

            configs.put(rootKey.key(), cfg);
        });
    }

    /**
     * Collects all schemas and subschemas (recursively) from root keys, internal and polymorphic schema extensions.
     *
     * @param rootKeys                    root keys
     * @param internalSchemaExtensions    internal schema extensions
     * @param polymorphicSchemaExtensions polymorphic schema extensions
     * @return set of all schema classes
     */
    private Set<Class<?>> collectAllSchemas(Collection<RootKey<?, ?>> rootKeys,
                                            Collection<Class<?>> internalSchemaExtensions,
                                            Collection<Class<?>> polymorphicSchemaExtensions) {
        Set<Class<?>> allSchemas = new HashSet<>();

        allSchemas.addAll(collectSchemas(viewReadOnly(rootKeys, RootKey::schemaClass)));
        allSchemas.addAll(collectSchemas(internalSchemaExtensions));
        allSchemas.addAll(collectSchemas(polymorphicSchemaExtensions));

        return allSchemas;
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

        storageRevisionListeners.clear();
    }

    /**
     * Initializes the configuration storage - reads data and sets default values for missing configuration properties.
     */
    public void initializeDefaults() {
        changer.initializeDefaults();

        for (RootKey<?, ?> rootKey : rootKeys) {
            DynamicConfiguration<?, ?> dynCfg = configs.get(rootKey.key());

            touch(dynCfg);
        }
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

        Object node;
        try {
            node = ConfigurationUtil.find(path, superRoot, false);
        } catch (KeyNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage());
        }

        if (node instanceof TraversableTreeNode) {
            return ((TraversableTreeNode) node).accept(null, visitor);
        }

        assert node == null || node instanceof Serializable;

        return visitor.visitLeafNode(null, (Serializable) node);
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
     * Configuration change notifier.
     *
     * @param oldSuperRoot Old roots values. All these roots always belong to a single storage.
     * @param newSuperRoot New values for the same roots as in {@code oldRoot}.
     * @param storageRevision Revision of the storage.
     * @param notificationNumber Current configuration listener notification number.
     * @return Future that must signify when processing is completed.
     */
    private CompletableFuture<Void> notificator(
            @Nullable SuperRoot oldSuperRoot,
            SuperRoot newSuperRoot,
            long storageRevision,
            long notificationNumber
    ) {
        Collection<CompletableFuture<?>> futures = new ArrayList<>();

        newSuperRoot.traverseChildren(new ConfigurationVisitor<Void>() {
            /** {@inheritDoc} */
            @Override
            public Void visitInnerNode(String key, InnerNode newRoot) {
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

        futures.addAll(notifyStorageRevisionListeners(storageRevision, notificationNumber));

        if (futures.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // Map futures is only for logging errors.
        Function<CompletableFuture<?>, CompletableFuture<?>> mapping = fut -> fut.whenComplete((res, throwable) -> {
            if (throwable != null) {
                LOG.error("Failed to notify configuration listener.", throwable);
            }
        });

        CompletableFuture<?>[] resultFutures = futures.stream().map(mapping).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(resultFutures);
    }

    /** {@inheritDoc} */
    @Override
    public void listenUpdateStorageRevision(ConfigurationStorageRevisionListener listener) {
        storageRevisionListeners.addListener(listener, changer.notificationCount());
    }

    /** {@inheritDoc} */
    @Override
    public void stopListenUpdateStorageRevision(ConfigurationStorageRevisionListener listener) {
        storageRevisionListeners.removeListener(listener);
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
     * Get configuration schemas and their validated internal extensions with checks.
     *
     * @param allSchemas               All configuration schemas.
     * @param internalSchemaExtensions Internal extensions ({@link InternalConfiguration}) of configuration schemas ({@link
     *                                 ConfigurationRoot} and {@link Config}).
     * @return Mapping: original of the schema -> internal schema extensions.
     * @throws IllegalArgumentException If the schema extension is invalid.
     */
    private Map<Class<?>, Set<Class<?>>> internalExtensionsWithCheck(
            Set<Class<?>> allSchemas,
            Collection<Class<?>> internalSchemaExtensions
    ) {
        if (internalSchemaExtensions.isEmpty()) {
            return Map.of();
        }

        Map<Class<?>, Set<Class<?>>> internalExtensions = internalSchemaExtensions(internalSchemaExtensions);

        Set<Class<?>> notInAllSchemas = difference(internalExtensions.keySet(), allSchemas);

        if (!notInAllSchemas.isEmpty()) {
            throw new IllegalArgumentException(
                    "Internal extensions for which no parent configuration schemas were found: " + notInAllSchemas
            );
        }

        return internalExtensions;
    }

    /**
     * Get polymorphic extensions of configuration schemas with checks.
     *
     * @param allSchemas                  All configuration schemas.
     * @param polymorphicSchemaExtensions Polymorphic extensions ({@link PolymorphicConfigInstance}) of configuration schemas.
     * @return Mapping: polymorphic scheme -> extensions (instances) of polymorphic configuration.
     * @throws IllegalArgumentException If the schema extension is invalid.
     */
    private Map<Class<?>, Set<Class<?>>> polymorphicExtensionsWithCheck(
            Set<Class<?>> allSchemas,
            Collection<Class<?>> polymorphicSchemaExtensions
    ) {
        Map<Class<?>, Set<Class<?>>> polymorphicExtensionsByParent = polymorphicSchemaExtensions(polymorphicSchemaExtensions);

        Set<Class<?>> notInAllSchemas = difference(polymorphicExtensionsByParent.keySet(), allSchemas);

        if (!notInAllSchemas.isEmpty()) {
            throw new IllegalArgumentException(
                    "Polymorphic extensions for which no polymorphic configuration schemas were found: " + notInAllSchemas
            );
        }

        Collection<Class<?>> noPolymorphicExtensionsSchemas = allSchemas.stream()
                .filter(ConfigurationUtil::isPolymorphicConfig)
                .filter(not(polymorphicExtensionsByParent::containsKey))
                .collect(toList());

        if (!noPolymorphicExtensionsSchemas.isEmpty()) {
            throw new IllegalArgumentException(
                    "Polymorphic configuration schemas for which no extensions were found: " + noPolymorphicExtensionsSchemas
            );
        }

        checkPolymorphicConfigIds(polymorphicExtensionsByParent);

        for (Map.Entry<Class<?>, Set<Class<?>>> e : polymorphicExtensionsByParent.entrySet()) {
            Class<?> schemaClass = e.getKey();

            Field typeIdField = schemaFields(schemaClass).get(0);

            if (!isPolymorphicId(typeIdField)) {
                throw new IllegalArgumentException(String.format(
                        "First field in a polymorphic configuration schema must contain @%s: %s",
                        PolymorphicId.class,
                        schemaClass.getName()
                ));
            }
        }

        return polymorphicExtensionsByParent;
    }

    /**
     * Checks that there are no conflicts between ids of a polymorphic configuration and its extensions (instances).
     *
     * @param polymorphicExtensions Mapping: polymorphic scheme -> extensions (instances) of polymorphic configuration.
     * @throws IllegalArgumentException If a polymorphic configuration id conflict is found.
     * @see PolymorphicConfigInstance#value
     */
    private void checkPolymorphicConfigIds(Map<Class<?>, Set<Class<?>>> polymorphicExtensions) {
        // Mapping: id -> configuration schema.
        Map<String, Class<?>> ids = new HashMap<>();

        for (Map.Entry<Class<?>, Set<Class<?>>> e : polymorphicExtensions.entrySet()) {
            for (Class<?> schemaClass : e.getValue()) {
                String id = polymorphicInstanceId(schemaClass);
                Class<?> prev = ids.put(id, schemaClass);

                if (prev != null) {
                    throw new IllegalArgumentException("Found an id conflict for a polymorphic configuration [id="
                            + id + ", schemas=" + List.of(prev, schemaClass));
                }
            }

            ids.clear();
        }
    }

    private Collection<CompletableFuture<?>> notifyStorageRevisionListeners(long storageRevision, long notificationNumber) {
        // Lazy init.
        List<CompletableFuture<?>> futures = null;

        for (Iterator<ConfigurationStorageRevisionListener> it = storageRevisionListeners.listeners(notificationNumber); it.hasNext(); ) {
            if (futures == null) {
                futures = new ArrayList<>();
            }

            ConfigurationStorageRevisionListener listener = it.next();

            try {
                CompletableFuture<?> future = listener.onUpdate(storageRevision);

                assert future != null;

                if (future.isCompletedExceptionally() || future.isCancelled() || !future.isDone()) {
                    futures.add(future);
                }
            } catch (Throwable t) {
                futures.add(CompletableFuture.failedFuture(t));
            }
        }

        return futures == null ? List.of() : futures;
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
