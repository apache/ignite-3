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
package org.apache.ignite.configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.internal.util.ConfigurationUtil;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.NamedListNode;
import org.apache.ignite.configuration.tree.TraversableTreeNode;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;

/**
 * Class that handles configuration changes, by validating them, passing to storage and listening to storage updates.
 */
public class ConfigurationChanger {
    /** */
    private final ForkJoinPool pool = new ForkJoinPool(2);

    /** Map of configurations' configurators. */
    @Deprecated
    private Map<RootKey<?>, Configurator<?>> configurators = new HashMap<>();

    /** */
    private final Map<Class<? extends ConfigurationStorage>, StorageRoots> storagesRootsMap = new ConcurrentHashMap<>();

    /** */
    public static class StorageRoots {
        /** */
        private final Map<RootKey<?>, InnerNode> roots;

        /** */
        private final int version;

        /** */
        private StorageRoots(Map<RootKey<?>, InnerNode> roots, int version) {
            this.roots = Collections.unmodifiableMap(roots);
            this.version = version;
        }
    }

    /** */
    private final Map<RootKey<?>, ConfigurationStorage> storages = new HashMap<>();

    /** */
    private final Map<Class<? extends ConfigurationStorage>, ConfigurationStorage> storageInstances = new HashMap<>();

    /** Constructor. */
    public ConfigurationChanger(ConfigurationStorage... configurationStorages) {
        for (ConfigurationStorage storage : configurationStorages)
            storageInstances.put(storage.getClass(), storage);
    }

    /**
     * Initialize changer.
     */
    // ConfigurationChangeException, really?
    public void init(RootKey<?>... rootKeys) throws ConfigurationChangeException {
        Map<Class<? extends ConfigurationStorage>, Set<RootKey<?>>> rootsByStorage = new HashMap<>();

        for (RootKey<?> rootKey : rootKeys) {
            Class<? extends ConfigurationStorage> storageType = rootKey.getStorageType();

            storages.put(rootKey, storageInstances.get(storageType));

            rootsByStorage.computeIfAbsent(storageType, c -> new HashSet<>()).add(rootKey);
        }

        for (ConfigurationStorage configurationStorage : storageInstances.values()) {
            Data data;

            try {
                data = configurationStorage.readAll();
            }
            catch (StorageException e) {
                throw new ConfigurationChangeException("Failed to initialize configuration: " + e.getMessage(), e);
            }

            Map<RootKey<?>, InnerNode> storageRootsMap = new HashMap<>();

            Map<String, ?> dataValuesPrefixMap = ConfigurationUtil.toPrefixMap(data.values());

            for (RootKey<?> rootKey : rootsByStorage.get(configurationStorage.getClass())) {
                Map<String, ?> rootPrefixMap = (Map<String, ?>)dataValuesPrefixMap.get(rootKey.key());

                if (rootPrefixMap == null) {
                    //TODO IGNITE-14193 Init with defaults.
                    storageRootsMap.put(rootKey, rootKey.createRootNode());
                }
                else {
                    InnerNode rootNode = rootKey.createRootNode();

                    ConfigurationUtil.fillFromPrefixMap(rootNode, rootPrefixMap);

                    storageRootsMap.put(rootKey, rootNode);
                }
            }

            storagesRootsMap.put(configurationStorage.getClass(), new StorageRoots(storageRootsMap, data.version()));

            configurationStorage.addListener(changedEntries -> updateFromListener(
                configurationStorage.getClass(),
                changedEntries
            ));

            // TODO: IGNITE-14118 iterate over data and fill Configurators
        }
    }

    /**
     * Add configurator.
     * @param key Root configuration key of the configurator.
     * @param configurator Configuration's configurator.
     */
    //TODO IGNITE-14183 Refactor, get rid of configurator and create some "validator".
    public void registerConfiguration(RootKey<?> key, Configurator<?> configurator) {
        configurators.put(key, configurator);
    }

    /**
     * Get root node by root key. Subject to revisiting.
     *
     * @param rootKey Root key.
     */
    public TraversableTreeNode getRootNode(RootKey<?> rootKey) {
        return this.storagesRootsMap.get(rootKey.getStorageType()).roots.get(rootKey);
    }

    /**
     * Change configuration.
     * @param changes Map of changes by root key.
     * @param storagesType
     */
    public CompletableFuture<Void> change(Map<RootKey<?>, TraversableTreeNode> changes) {
        if (changes.isEmpty())
            return CompletableFuture.completedFuture(null);

        Set<Class<? extends ConfigurationStorage>> storagesTypes = changes.keySet().stream()
            .map(RootKey::getStorageType)
            .collect(Collectors.toSet());

        assert !storagesTypes.isEmpty();

        if (storagesTypes.size() != 1) {
            return CompletableFuture.failedFuture(
                new ConfigurationChangeException("Cannot change configurations belonging to different roots")
            );
        }

        Class<? extends ConfigurationStorage> storageType = storagesTypes.iterator().next();

        CompletableFuture<Void> fut = new CompletableFuture<>();

        pool.execute(() -> change0(changes, storageType, fut));

        return fut;
    }

    /** */
    private void change0(
        Map<RootKey<?>, TraversableTreeNode> changes,
        Class<? extends ConfigurationStorage> storageType,
        CompletableFuture<?> fut
    ) {
        Map<String, Serializable> allChanges = changes.entrySet().stream()
            .map((Map.Entry<RootKey<?>, TraversableTreeNode> change) -> convertChangesToMap(change.getKey(), change.getValue()))
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        StorageRoots roots = storagesRootsMap.get(storageType);

        final ValidationResult validationResult = validate(roots, changes);

        List<ValidationIssue> validationIssues = validationResult.issues();

        if (!validationIssues.isEmpty()) {
            fut.completeExceptionally(new ConfigurationValidationException(validationIssues));

            return;
        }

        final int version = validationResult.version();

        CompletableFuture<Boolean> writeFut = storageInstances.get(storageType).write(allChanges, version);

        writeFut.whenCompleteAsync((casResult, throwable) -> {
            if (throwable != null)
                fut.completeExceptionally(new ConfigurationChangeException("Failed to change configuration", throwable));
            else if (casResult)
                fut.complete(null);
            else
                change0(changes, storageType, fut);
        }, pool);
    }

    /**
     * Update configuration from storage listener.
     * @param storageType
     * @param changedEntries Changed data.
     */
    private void updateFromListener(
        Class<? extends ConfigurationStorage> storageType,
        Data changedEntries
    ) {
        StorageRoots oldStorageRoots = this.storagesRootsMap.get(storageType);

        Map<RootKey<?>, InnerNode> storageRootsMap = new HashMap<>(oldStorageRoots.roots);

        Map<String, ?> dataValuesPrefixMap = ConfigurationUtil.toPrefixMap(changedEntries.values());

        for (RootKey<?> rootKey : oldStorageRoots.roots.keySet()) {
            //TODO IGNITE-14182 Remove is not yet supported here.
            Map<String, ?> rootPrefixMap = (Map<String, ?>)dataValuesPrefixMap.get(rootKey.key());

            if (rootPrefixMap != null) {
                InnerNode rootNode = oldStorageRoots.roots.get(rootKey).copy();

                ConfigurationUtil.fillFromPrefixMap(rootNode, rootPrefixMap);

                storageRootsMap.put(rootKey, rootNode);
            }
        }

        StorageRoots storageRoots = new StorageRoots(storageRootsMap, changedEntries.version());

        storagesRootsMap.put(storageType, storageRoots);

        // Notify listeners.
    }

    /**
     * Validate configuration changes.
     *
     * @param storageRoots Storage roots.
     * @param changes Configuration changes.
     * @return Validation results.
     */
    private ValidationResult validate(
        StorageRoots storageRoots,
        Map<RootKey<?>, TraversableTreeNode> changes
    ) {
        List<ValidationIssue> issues = new ArrayList<>();

        for (Map.Entry<RootKey<?>, TraversableTreeNode> entry : changes.entrySet()) {
            RootKey<?> rootKey = entry.getKey();
            TraversableTreeNode changesForRoot = entry.getValue();

            final Configurator<?> configurator = configurators.get(rootKey);

            List<ValidationIssue> list = configurator.validateChanges(changesForRoot);
            issues.addAll(list);
        }

        // Why would I put a version into a validation result? Makes no sense.
        return new ValidationResult(issues, storageRoots.version);
    }

    /**
     * Convert a traversable tree to a map of qualified keys to values.
     * @param rootKey Root configuration key.
     * @param node Tree.
     * @return Map of changes.
     */
    private Map<String, Serializable> convertChangesToMap(RootKey<?> rootKey, TraversableTreeNode node) {
        Map<String, Serializable> values = new HashMap<>();

        node.accept(rootKey.key(), new ConfigurationVisitor<>() {
            /** Current key, aggregated by visitor. */
            StringBuilder currentKey = new StringBuilder();

            /** {@inheritDoc} */
            @Override public Void visitLeafNode(String key, Serializable val) {
                if (val != null)
                    values.put(currentKey.toString() + key, val);

                return null;
            }

            /** {@inheritDoc} */
            @Override public Void visitInnerNode(String key, InnerNode node) {
                if (node == null)
                    return null;

                int previousKeyLength = currentKey.length();

                currentKey.append(key).append('.');

                node.traverseChildren(this);

                currentKey.setLength(previousKeyLength);

                return null;
            }

            /** {@inheritDoc} */
            @Override public <N extends InnerNode> Void visitNamedListNode(String key, NamedListNode<N> node) {
                int previousKeyLength = currentKey.length();

                if (key != null)
                    currentKey.append(key).append('.');

                for (String namedListKey : node.namedListKeys()) {
                    int loopPreviousKeyLength = currentKey.length();

                    currentKey.append(ConfigurationUtil.escape(namedListKey)).append('.');

                    node.get(namedListKey).traverseChildren(this);

                    currentKey.setLength(loopPreviousKeyLength);
                }

                currentKey.setLength(previousKeyLength);

                return null;
            }
        });

        return values;
    }

    /**
     * Results of the validation.
     */
    private static final class ValidationResult {
        /** List of issues. */
        private final List<ValidationIssue> issues;

        /** Version of configuration that changes were validated against. */
        private final int version;

        /**
         * Constructor.
         * @param issues List of issues.
         * @param version Version.
         */
        private ValidationResult(List<ValidationIssue> issues, int version) {
            this.issues = issues;
            this.version = version;
        }

        /**
         * Get issues.
         * @return Issues.
         */
        public List<ValidationIssue> issues() {
            return issues;
        }

        /**
         * Get version.
         * @return Version.
         */
        public int version() {
            return version;
        }
    }
}
