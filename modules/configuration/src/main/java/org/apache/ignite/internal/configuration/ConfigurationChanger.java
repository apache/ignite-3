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

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.validation.MemberKey;
import org.apache.ignite.internal.configuration.validation.ValidationUtil;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationFlattener.createFlattenedUpdatesMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.checkConfigurationType;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.dropNulls;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.fillFromPrefixMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.toPrefixMap;

/**
 * Class that handles configuration changes, by validating them, passing to storage and listening to storage updates.
 */
public abstract class ConfigurationChanger {
    /** Thread pool. */
    private final ForkJoinPool pool = new ForkJoinPool(2);

    /** Lazy annotations cache for configuration schema fields. */
    private final Map<MemberKey, Annotation[]> cachedAnnotations = new ConcurrentHashMap<>();

    /** Closure to execute when an update from the storage is received. */
    private final Notificator notificator;

    /** Root keys. Mapping: {@link RootKey#key()} -> identity (itself). */
    private final Map<String, RootKey<?, ?>> rootKeys;

    /** Validators. */
    private final Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators;

    /** Configuration storage. */
    private final ConfigurationStorage storage;

    /** Storage trees. */
    private volatile StorageRoots storageRoots;

    /**
     * Closure interface to be used by the configuration changer. An instance of this closure is passed into
     * the constructor and invoked every time when there's an update from any of the storages.
     */
    @FunctionalInterface
    public interface Notificator {
        /**
         * Invoked every time when the configuration is updated.
         *
         * @param oldRoot Old roots values. All these roots always belong to a single storage.
         * @param newRoot New values for the same roots as in {@code oldRoot}.
         * @param storageRevision Revision of the storage.
         * @return Not-null future that must signify when processing is completed. Exceptional completion is not
         *      expected.
         */
        @NotNull CompletableFuture<Void> notify(SuperRoot oldRoot, SuperRoot newRoot, long storageRevision);
    }

    /**
     * Immutable data container to store version and all roots associated with the specific storage.
     */
    private static class StorageRoots {
        /** Immutable forest, so to say. */
        private final SuperRoot roots;

        /** Version associated with the currently known storage state. */
        private final long version;

        /** Future that signifies update of current configuration. */
        private final CompletableFuture<Void> changeFuture = new CompletableFuture<>();

        /**
         * Constructor.
         *
         * @param roots Forest.
         * @param version Version associated with the currently known storage state.
         */
        private StorageRoots(SuperRoot roots, long version) {
            this.roots = roots;
            this.version = version;
        }
    }

    /**
     * Constructor.
     *
     * @param notificator Closure to execute when update from the storage is received.
     * @param rootKeys Configuration root keys.
     * @param validators Validators.
     * @param storage Configuration storage.
     * @throws IllegalArgumentException If the configuration type of the root keys is not equal to the storage type.
     */
    public ConfigurationChanger(
        Notificator notificator,
        Collection<RootKey<?, ?>> rootKeys,
        Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators,
        ConfigurationStorage storage
    ) {
        checkConfigurationType(rootKeys, storage);

        this.notificator = notificator;
        this.validators = validators;
        this.storage = storage;

        this.rootKeys = rootKeys.stream().collect(toMap(RootKey::key, identity()));
    }

    /**
     * Creates new {@code Node} object that corresponds to passed root keys root configuration node.
     *
     * @param rootKey Root key.
     * @return New {@link InnerNode} instance that represents root.
     */
    public abstract InnerNode createRootNode(RootKey<?, ?> rootKey);

    /**
     * Utility method to create {@link SuperRoot} parameter value.
     *
     * @return Function that creates root node by root name or returns {@code null} if root name is not found.
     */
    private Function<String, InnerNode> rootCreator() {
        return key -> {
            RootKey<?, ?> rootKey = rootKeys.get(key);

            return rootKey == null ? null : createRootNode(rootKey);
        };
    }

    /**
     * Start component.
     */
    // ConfigurationChangeException, really?
    public void start() throws ConfigurationChangeException {
        Data data;

        try {
            data = storage.readAll();
        }
        catch (StorageException e) {
            throw new ConfigurationChangeException("Failed to initialize configuration: " + e.getMessage(), e);
        }

        SuperRoot superRoot = new SuperRoot(rootCreator());

        Map<String, ?> dataValuesPrefixMap = toPrefixMap(data.values());

        for (RootKey<?, ?> rootKey : rootKeys.values()) {
            Map<String, ?> rootPrefixMap = (Map<String, ?>)dataValuesPrefixMap.get(rootKey.key());

            InnerNode rootNode = createRootNode(rootKey);

            if (rootPrefixMap != null)
                fillFromPrefixMap(rootNode, rootPrefixMap);

            superRoot.addRoot(rootKey, rootNode);
        }

        storageRoots = new StorageRoots(superRoot, data.changeId());

        storage.registerConfigurationListener(this::updateFromListener);
    }

    /**
     * Initializes the configuration storage - reads data and sets default values for missing configuration properties.
     *
     * @throws ConfigurationValidationException If configuration validation failed.
     * @throws ConfigurationChangeException If configuration framework failed to add default values and save them to storage.
     */
    public void initializeDefaults() throws ConfigurationValidationException, ConfigurationChangeException {
        try {
            ConfigurationSource defaultsCfgSource = new ConfigurationSource() {
                /** {@inheritDoc} */
                @Override public void descend(ConstructableTreeNode node) {
                    addDefaults((InnerNode)node);
                }
            };

            changeInternally(defaultsCfgSource).get();
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof ConfigurationValidationException)
                throw (ConfigurationValidationException)cause;

            if (cause instanceof ConfigurationChangeException)
                throw (ConfigurationChangeException)cause;

            throw new ConfigurationChangeException(
                "Failed to write default configuration values into the storage " + storage.getClass(), e
            );
        }
        catch (InterruptedException e) {
            throw new ConfigurationChangeException(
                "Failed to initialize configuration storage " + storage.getClass(), e
            );
        }
    }

    /**
     * Changes the configuration.
     *
     * @param source Configuration source to create patch from.
     * @return Future that is completed on change completion.
     */
    public CompletableFuture<Void> change(ConfigurationSource source) {
        return changeInternally(source);
    }

    /** Stop component. */
    public void stop() {
        pool.shutdownNow();

        StorageRoots roots = storageRoots;

        if (roots != null)
            roots.changeFuture.completeExceptionally(new NodeStoppingException());
    }

    /**
     * Get root node by root key. Subject to revisiting.
     *
     * @param rootKey Root key.
     * @return Root node.
     */
    public InnerNode getRootNode(RootKey<?, ?> rootKey) {
        return storageRoots.roots.getRoot(rootKey);
    }

    /**
     * Get storage super root.
     *
     * @return Super root storage.
     */
    public SuperRoot superRoot() {
        return storageRoots.roots;
    }

    /**
     * Internal configuration change method that completes provided future.
     *
     * @param src Configuration source.
     * @return fut Future that will be completed after changes are written to the storage.
     */
    private CompletableFuture<Void> changeInternally(ConfigurationSource src) {
        StorageRoots localRoots = storageRoots;

        return CompletableFuture
            .supplyAsync(() -> {
                SuperRoot curRoots = localRoots.roots;

                SuperRoot changes = curRoots.copy();

                src.reset();

                src.descend(changes);

                addDefaults(changes);

                Map<String, Serializable> allChanges = createFlattenedUpdatesMap(curRoots, changes);

                // Unlikely but still possible.
                if (allChanges.isEmpty())
                    return null;

                dropNulls(changes);

                List<ValidationIssue> validationIssues = ValidationUtil.validate(
                    curRoots,
                    changes,
                    this::getRootNode,
                    cachedAnnotations,
                    validators
                );

                if (!validationIssues.isEmpty())
                    throw new ConfigurationValidationException(validationIssues);

                return allChanges;
            }, pool)
            .thenCompose(allChanges -> {
                if (allChanges == null)
                    return completedFuture(null);

                return storage.write(allChanges, localRoots.version)
                    .thenCompose(casResult -> {
                        if (casResult)
                            return localRoots.changeFuture;
                        else
                            return localRoots.changeFuture.thenCompose(v -> changeInternally(src));
                    })
                    .exceptionally(throwable -> {
                        throw new ConfigurationChangeException("Failed to change configuration", throwable);
                    });
            });
    }

    /**
     * Updates configuration from storage listener.
     *
     * @param changedEntries Changed data.
     * @return Future that signifies update completion.
     */
    private CompletableFuture<Void> updateFromListener(Data changedEntries) {
        StorageRoots oldStorageRoots = storageRoots;

        Map<String, ?> dataValuesPrefixMap = toPrefixMap(changedEntries.values());

        compressDeletedEntries(dataValuesPrefixMap);

        SuperRoot oldSuperRoot = oldStorageRoots.roots;
        SuperRoot newSuperRoot = oldSuperRoot.copy();

        fillFromPrefixMap(newSuperRoot, dataValuesPrefixMap);

        long newChangeId = changedEntries.changeId();

        storageRoots = new StorageRoots(newSuperRoot, newChangeId);

        return notificator.notify(oldSuperRoot, newSuperRoot, newChangeId)
            .whenComplete((v, t) -> {
                if (t == null)
                    oldStorageRoots.changeFuture.complete(null);
                else
                    oldStorageRoots.changeFuture.completeExceptionally(t);
            });
    }

    /**
     * "Compress" prefix map - this means that deleted named list elements will be represented as a single {@code null}
     * objects instead of a number of nullified configuration leaves.
     *
     * @param prefixMap Prefix map, constructed from the storage notification data or its subtree.
     */
    private void compressDeletedEntries(Map<String, ?> prefixMap) {
        // Here we basically assume that if prefix subtree contains single null child then all its childrens are nulls.
        // Replace all such elements will nulls, signifying that these are deleted named list elements.
        prefixMap.replaceAll((key, value) ->
            value instanceof Map && ((Map<?, ?>)value).containsValue(null) ? null : value
        );

        // Continue recursively.
        for (Object value : prefixMap.values()) {
            if (value instanceof Map)
                compressDeletedEntries((Map<String, ?>)value);
        }
    }
}
