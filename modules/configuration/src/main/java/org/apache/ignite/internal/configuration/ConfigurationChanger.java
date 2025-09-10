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

import static java.util.Collections.emptyNavigableMap;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.function.Function.identity;
import static java.util.regex.Pattern.quote;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.DeletedKeysFilter.ignoreDeleted;
import static org.apache.ignite.internal.configuration.direct.KeyPathNode.INTERNAL_IDS;
import static org.apache.ignite.internal.configuration.tree.InnerNode.INJECTED_NAME;
import static org.apache.ignite.internal.configuration.tree.InnerNode.INTERNAL_ID;
import static org.apache.ignite.internal.configuration.util.ConfigurationFlattener.createFlattenedUpdatesMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.KEY_SEPARATOR;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.checkConfigurationType;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.compressDeletedEntries;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.dropNulls;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.escape;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.fillFromPrefixMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.findEx;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.ignoreLegacyKeys;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.toPrefixMap;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.KeyIgnorer;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.configuration.direct.KeyPathNode;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Lazy;
import org.jetbrains.annotations.Nullable;

/**
 * Class that handles configuration changes, by validating them, passing to storage and listening to storage updates.
 */
public abstract class ConfigurationChanger implements DynamicConfigurationChanger {
    /** Thread pool. */
    private final ForkJoinPool pool = new ForkJoinPool(2);

    /** Closure to execute when an update from the storage is received. */
    private final ConfigurationUpdateListener configurationUpdateListener;

    /** Root keys. Mapping: {@link RootKey#key()} -> identity (itself). */
    private final Map<String, RootKey<?, ?, ?>> rootKeys;

    private final Lazy<Map<String, Serializable>> defaultsMap = new Lazy<>(this::createDefaultsMap);

    /** Configuration storage. */
    private final ConfigurationStorage storage;

    /** Configuration validator. */
    private final ConfigurationValidator configurationValidator;

    /** Configuration migrator. */
    private final ConfigurationMigrator migrator;

    /** Determines if key should be ignored. */
    private final KeyIgnorer keyIgnorer;

    /** Storage trees. */
    private volatile StorageRoots storageRoots;

    /**
     * Initial configuration. This configuration will be used to initialize the configuration if the revision of the storage is {@code 0}.
     * If the revision of the storage is non-zero, this configuration will be ignored.
     */
    private volatile ConfigurationSource initialConfiguration = ConfigurationUtil.EMPTY_CFG_SRC;

    /** Future that resolves after the defaults are persisted to the storage. */
    private final CompletableFuture<Void> defaultsPersisted = new CompletableFuture<>();

    /** Configuration listener notification counter, must be incremented before each use of {@link #configurationUpdateListener}. */
    private final AtomicLong notificationListenerCnt = new AtomicLong();

    /** Lock for reading/updating the {@link #storageRoots}. Fair, to give a higher priority to external updates. */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    /** Flag indicating whether the component is started. */
    private volatile boolean started = false;

    /** Keys that were deleted from the configuration, but were present in the storage. Will be deleted on startup. */
    private Collection<String> ignoredKeys;

    /**
     * Closure interface to be used by the configuration changer. An instance of this closure is passed into the constructor and invoked
     * every time when there's an update from any of the storages.
     */
    public interface ConfigurationUpdateListener {
        /**
         * Invoked every time when the configuration is updated.
         *
         * @param oldRoot Old roots values. All these roots always belong to a single storage.
         * @param newRoot New values for the same roots as in {@code oldRoot}.
         * @param storageRevision Configuration revision of the storage.
         * @param notificationNumber Configuration listener notification number.
         * @return Future that must signify when processing is completed. Exceptional completion is not expected.
         */
        CompletableFuture<Void> onConfigurationUpdated(
                @Nullable SuperRoot oldRoot, SuperRoot newRoot, long storageRevision, long notificationNumber
        );
    }

    /**
     * Immutable data container to store version and all roots associated with the specific storage.
     */
    private static class StorageRoots {
        /** Immutable forest, so to say. */
        private final SuperRoot rootsWithoutDefaults;

        /** Immutable forest, so to say. */
        private final SuperRoot roots;

        /** Change ID that corresponds to this state. */
        private final long changeId;

        /** Full storage data. */
        private final NavigableMap<String, ? extends Serializable> storageData;

        /** Future that signifies update of current configuration. */
        private final CompletableFuture<Void> changeFuture = new CompletableFuture<>();

        /**
         * Constructor.
         *
         * @param rootsWithoutDefaults Forest without the defaults
         * @param roots Forest with the defaults filled in
         * @param changeId Change ID that corresponds to this state.
         * @param storageData Full storage data.
         */
        private StorageRoots(
                SuperRoot rootsWithoutDefaults,
                SuperRoot roots,
                long changeId,
                NavigableMap<String, ? extends Serializable> storageData
        ) {
            this.rootsWithoutDefaults = rootsWithoutDefaults;
            this.roots = roots;
            this.changeId = changeId;
            this.storageData = storageData;

            makeImmutable(roots);
            makeImmutable(rootsWithoutDefaults);
        }
    }

    /**
     * Makes the node immutable by calling {@link ConstructableTreeNode#makeImmutable()} on each sub-node recursively.
     */
    private static void makeImmutable(InnerNode node) {
        if (node == null || !node.makeImmutable()) {
            return;
        }

        node.traverseChildren(new ConfigurationVisitor<>() {
            @Override
            public @Nullable Object visitInnerNode(Field field, String key, InnerNode node) {
                makeImmutable(node);

                return null;
            }

            @Override
            public @Nullable Object visitNamedListNode(Field field, String key, NamedListNode<?> node) {
                if (node.makeImmutable()) {
                    for (String namedListKey : node.namedListKeys()) {
                        makeImmutable(node.getInnerNode(namedListKey));
                    }
                }

                return null;
            }
        }, true);
    }

    /**
     * Constructor.
     *
     * @param configurationUpdateListener Closure to execute when update from the storage is received.
     * @param rootKeys Configuration root keys.
     * @param storage Configuration storage.
     * @param configurationValidator Configuration validator.
     * @param migrator Configuration migrator.
     * @param keyIgnorer Determines if key should be ignored.
     * @throws IllegalArgumentException If the configuration type of the root keys is not equal to the storage type.
     */
    public ConfigurationChanger(
            ConfigurationUpdateListener configurationUpdateListener,
            Collection<RootKey<?, ?, ?>> rootKeys,
            ConfigurationStorage storage,
            ConfigurationValidator configurationValidator,
            ConfigurationMigrator migrator,
            KeyIgnorer keyIgnorer
    ) {
        checkConfigurationType(rootKeys, storage);

        this.configurationUpdateListener = configurationUpdateListener;
        this.storage = storage;
        this.configurationValidator = configurationValidator;
        this.rootKeys = rootKeys.stream().collect(toMap(RootKey::key, identity()));
        this.migrator = migrator;

        this.keyIgnorer = keyIgnorer;
    }

    /**
     * Creates new {@code Node} object that corresponds to passed root keys root configuration node.
     *
     * @param rootKey Root key.
     * @return New {@link InnerNode} instance that represents root.
     */
    public abstract InnerNode createRootNode(RootKey<?, ?, ?> rootKey);

    /**
     * Utility method to create {@link SuperRoot} parameter value.
     *
     * @return Function that creates root node by root name or returns {@code null} if root name is not found.
     */
    private Function<String, RootInnerNode> rootCreator() {
        return key -> {
            RootKey<?, ?, ?> rootKey = rootKeys.get(key);

            return rootKey == null ? null : new RootInnerNode(rootKey, createRootNode(rootKey));
        };
    }

    /**
     * Start component.
     */
    public void start() {
        Data data;

        try {
            data = storage.readDataOnRecovery().get();
        } catch (ExecutionException e) {
            throw new ConfigurationChangeException("Failed to initialize configuration: " + e.getCause().getMessage(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new ConfigurationChangeException("Failed to initialize configuration: " + e.getMessage(), e);
        }

        var storageValues = new HashMap<String, Serializable>(data.values());

        ignoredKeys = ignoreDeleted(storageValues, keyIgnorer);

        long revision = data.changeId();

        SuperRoot superRoot = new SuperRoot(rootCreator());

        Map<String, ?> dataValuesPrefixMap = toPrefixMap(storageValues);

        for (RootKey<?, ?, ?> rootKey : rootKeys.values()) {
            Map<String, ?> rootPrefixMap = (Map<String, ?>) dataValuesPrefixMap.get(rootKey.key());

            InnerNode rootNode = createRootNode(rootKey);

            if (rootPrefixMap != null) {
                fillFromPrefixMap(rootNode, rootPrefixMap);
            }

            superRoot.addRoot(rootKey, rootNode);
        }

        // Create a copy of the super root, excluding the initial configuration, for saving with the defaults.
        SuperRoot superRootNoDefaults = superRoot.copy();

        addDefaults(superRoot);

        // Fill the configuration with the initial configuration.
        if (revision == 0) {
            initialConfiguration.descend(superRoot);
        }

        // Validate the restored configuration.
        validateConfiguration(superRoot);
        // We store two configuration roots, one with the defaults set and another one without them.
        // The root WITH the defaults is used when we calculate who to notify of a configuration change or
        // when we provide the configuration outside.
        // The root WITHOUT the defaults is used to calculate which properties to write to the underlying storage,
        // in other words it allows us to persist the defaults from the code.
        // After the storage listener fires for the first time both roots are supposed to become equal.
        storageRoots = new StorageRoots(superRootNoDefaults, superRoot, data.changeId(), new TreeMap<>(data.values()));

        storage.registerConfigurationListener(configurationStorageListener());

        persistModifiedConfiguration();

        started = true;
    }

    /**
     * Persists configuration to the storage on startup.
     *
     * <p>Specifically, this method runs a check whether there are
     * default values that are not persisted to the storage and writes them if there are any, or deletes values that were deprecated.
     */
    private void persistModifiedConfiguration() {
        // If the storage version is 0, it indicates that the storage is empty.
        // In this case, write the defaults along with the initial configuration.
        ConfigurationSource cfgSrc = storageRoots.changeId == 0 ? initialConfiguration : ConfigurationUtil.EMPTY_CFG_SRC;

        changeInternally(cfgSrc, true)
                .whenComplete((v, e) -> {
                    if (e == null) {
                        defaultsPersisted.complete(null);
                    } else {
                        defaultsPersisted.completeExceptionally(e);
                    }
                });
    }

    /**
     * Sets {@link #initialConfiguration}. This configuration will be used to initialize the configuration if the revision of the storage is
     * {@code 0}. If the revision of the storage is non-zero, this configuration will be ignored. his method must be called before
     * {@link #start()}. If the method is not called, the initial configuration will be empty.
     *
     * @param configurationSource the configuration source to initialize with.
     */
    public void initializeConfigurationWith(ConfigurationSource configurationSource) {
        assert !started : "ConfigurationChanger#initializeConfigurationWith must be called before the start.";

        initialConfiguration = configurationSource;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> change(ConfigurationSource source) {
        if (storageRoots == null) {
            throw new ComponentNotStartedException();
        }
        return defaultsPersisted.thenCompose(v -> changeInternally(source, false));
    }

    /**
     * Returns a future that resolves after the defaults are persisted to the storage.
     */
    public CompletableFuture<Void> onDefaultsPersisted() {
        return defaultsPersisted;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T getLatest(List<KeyPathNode> path) {
        assert !path.isEmpty();
        assert path instanceof RandomAccess : path.getClass();
        assert !path.get(0).unresolvedName : path;

        // This map will be merged into the data from the storage. It's required for the conversion into tree to work.
        // Namely, named list order indexes and names are mandatory for conversion.
        Map<String, Map<String, Serializable>> extras = new HashMap<>();

        // Joiner for the prefix that will be used to fetch data from the storage.
        StringJoiner prefixJoiner = new StringJoiner(KEY_SEPARATOR);

        int pathSize = path.size();

        KeyPathNode lastPathNode = path.get(pathSize - 1);

        // This loop is required to accumulate prefix and resolve all unresolved named list elements' ids.
        for (int idx = 0; idx < pathSize; idx++) {
            KeyPathNode keyPathNode = path.get(idx);

            // Regular keys and resolved ids go straight to the prefix.
            if (!keyPathNode.unresolvedName) {
                // Fake name and 0 index go to extras in case of resolved named list elements.
                if (keyPathNode.namedListEntry) {
                    prefixJoiner.add(keyPathNode.key);

                    String prefix = prefixJoiner + KEY_SEPARATOR;

                    extras.put(prefix, Map.of(
                            prefix + NamedListNode.NAME, "<name_placeholder>",
                            prefix + NamedListNode.ORDER_IDX, 0
                    ));
                } else {
                    prefixJoiner.add(keyPathNode.key);
                }

                continue;
            }

            assert keyPathNode.namedListEntry : path;

            // Here we have unresolved named list element. Name must be translated into the internal id.
            // There's a special path for this purpose in the storage.
            String unresolvedNameKey = prefixJoiner + KEY_SEPARATOR
                    + NamedListNode.IDS + KEY_SEPARATOR
                    + escape(keyPathNode.key);

            // Data from the storage.
            Serializable resolvedName = get(storage.readLatest(unresolvedNameKey));

            if (resolvedName == null) {
                throw new NoSuchElementException(prefixJoiner + KEY_SEPARATOR + escape(keyPathNode.key));
            }

            assert resolvedName instanceof UUID : resolvedName;

            // Resolved internal id from the map.
            UUID internalId = (UUID) resolvedName;

            // There's a chance that this is exactly what user wants. If their request ends with
            // `*.get("resourceName").internalId()` then the result can be returned straight away.
            if (idx == pathSize - 2 && INTERNAL_ID.equals(lastPathNode.key)) {
                assert !lastPathNode.unresolvedName : path;

                // Despite the fact that this cast looks very stupid, it is correct. Internal ids are always UUIDs.
                return (T) internalId;
            }

            prefixJoiner.add(internalId.toString());

            String prefix = prefixJoiner + KEY_SEPARATOR;

            // Real name and 0 index go to extras in case of unresolved named list elements.
            extras.put(prefix, Map.of(
                    prefix + NamedListNode.NAME, keyPathNode.key,
                    prefix + NamedListNode.ORDER_IDX, 0
            ));
        }

        // Exceptional case, the only purpose of it is to ensure that named list element with given internal id does exist.
        // That id must be resolved, otherwise method would already be completed in the loop above.
        if (lastPathNode.key.equals(INTERNAL_ID) && !lastPathNode.unresolvedName && path.get(pathSize - 2).namedListEntry) {
            assert !path.get(pathSize - 2).unresolvedName : path;

            // Not very elegant, I know. <internal_id> is replaced with the <name> in the prefix.
            // <name> always exists in named list element, and it's an easy way to check element's existence.
            String nameStorageKey = prefixJoiner.toString().replaceAll(quote(INTERNAL_ID) + "$", NamedListNode.NAME);

            // Data from the storage.
            Serializable name = get(storage.readLatest(nameStorageKey));

            if (name != null) {
                // Id is already known.
                return (T) UUID.fromString(path.get(pathSize - 2).key);
            } else {
                throw new NoSuchElementException(prefixJoiner.toString());
            }
        }

        String prefix = prefixJoiner.toString();

        // Reading all ids is also a special case.
        if (lastPathNode.key.equals(INTERNAL_IDS) && !lastPathNode.unresolvedName && path.get(pathSize - 1).namedListEntry) {
            prefix = prefix.replaceAll(quote(INTERNAL_IDS) + "$", NamedListNode.IDS + KEY_SEPARATOR);

            Map<String, ? extends Serializable> storageData = get(storage.readAllLatest(prefix));

            return (T) List.copyOf(storageData.values());
        }

        if (lastPathNode.key.equals(INTERNAL_ID) && !path.get(pathSize - 2).namedListEntry) {
            // This is not particularly efficient, but there's no way someone will actually use this case for real outside of tests.
            prefix = prefix.replaceAll(quote(KEY_SEPARATOR + INTERNAL_ID) + "$", "");
        } else if (lastPathNode.key.contains(INJECTED_NAME)) {
            prefix = prefix.replaceAll(quote(KEY_SEPARATOR + INJECTED_NAME), "");
        }

        // Data from the storage.
        Map<String, ? extends Serializable> storageData = get(storage.readAllLatest(prefix));

        // Data to be converted into the tree.
        Map<String, Serializable> mergedData = new HashMap<>();

        if (!storageData.isEmpty()) {
            mergedData.putAll(storageData);

            for (Entry<String, Map<String, Serializable>> extrasEntry : extras.entrySet()) {
                for (String storageKey : storageData.keySet()) {
                    String extrasPrefix = extrasEntry.getKey();

                    if (storageKey.startsWith(extrasPrefix)) {
                        // Add extra order indexes and names before converting it to the tree.
                        for (Entry<String, Serializable> extrasEntryMap : extrasEntry.getValue().entrySet()) {
                            mergedData.putIfAbsent(extrasEntryMap.getKey(), extrasEntryMap.getValue());
                        }

                        break;
                    }
                }
            }

            if (lastPathNode.namedListEntry) {
                // Change element's order index to zero. Conversion won't work if indexes range is not continuous.
                mergedData.put(prefix + KEY_SEPARATOR + NamedListNode.ORDER_IDX, 0);
            }
        }

        // Super root that'll be filled from the storage data.
        InnerNode rootNode = new SuperRoot(rootCreator());

        fillFromPrefixMap(rootNode, toPrefixMap(mergedData));

        // "addDefaults" won't work if regular root is missing.
        if (storageData.isEmpty()) {
            rootNode.construct(path.get(0).key, ConfigurationUtil.EMPTY_CFG_SRC, true);
        }

        addDefaults(rootNode);

        return findEx(path, rootNode);
    }

    /** Stop component. */
    public void stop() {
        IgniteUtils.shutdownAndAwaitTermination(pool, 10, TimeUnit.SECONDS);

        defaultsPersisted.completeExceptionally(new NodeStoppingException());

        StorageRoots roots = storageRoots;

        if (roots != null) {
            roots.changeFuture.completeExceptionally(new NodeStoppingException());
        }

        storage.close();
    }

    /** {@inheritDoc} */
    @Override
    public InnerNode getRootNode(RootKey<?, ?, ?> rootKey) {
        return storageRoots.roots.getRoot(rootKey);
    }

    /**
     * Get storage super root.
     *
     * @return Super root storage.
     * @throws ComponentNotStartedException if changer is not started.
     */
    public SuperRoot superRoot() {
        StorageRoots localRoots = storageRoots;

        if (localRoots == null) {
            throw new ComponentNotStartedException();
        }

        return localRoots.roots;
    }

    /**
     * Entry point for configuration changes.
     *
     * @param src Configuration source.
     * @param onStartup if {@code true} this change is triggered right after startup
     * @return Future that will be completed after changes are written to the storage.
     * @throws ComponentNotStartedException if changer is not started.
     */
    private CompletableFuture<Void> changeInternally(ConfigurationSource src, boolean onStartup) {
        return storage.lastRevision()
                .thenComposeAsync(storageRevision -> {
                    assert storageRevision != null;

                    return changeInternally0(src, storageRevision, onStartup);
                }, pool)
                .exceptionally(throwable -> {
                    Throwable cause = throwable.getCause();

                    if (cause instanceof ConfigurationChangeException) {
                        throw ((ConfigurationChangeException) cause);
                    } else {
                        throw new ConfigurationChangeException("Failed to change configuration", cause);
                    }
                });
    }

    /**
     * Internal configuration change method that completes provided future.
     *
     * @param src Configuration source.
     * @param storageRevision Latest storage revision from the metastorage.
     * @param onStartup if {@code true} this change is triggered right after startup
     * @return Future that will be completed after changes are written to the storage.
     */
    private CompletableFuture<Void> changeInternally0(ConfigurationSource src, long storageRevision, boolean onStartup) {
        // Read lock protects "storageRoots" field from being updated, thus guaranteeing a thread-safe read of configuration inside the
        // change closure.
        rwLock.readLock().lock();

        try {
            // Put it into a variable to avoid volatile reads.
            // This read and the following comparison MUST be performed while holding read lock.
            StorageRoots localRoots = storageRoots;

            if (localRoots.changeId < storageRevision) {
                // Need to wait for the configuration updates from the storage, then try to update again (loop).
                return localRoots.changeFuture.thenCompose(v -> changeInternally(src, onStartup));
            }

            SuperRoot curRoots = localRoots.roots;

            SuperRoot changes = curRoots.copy();

            src.reset();

            src.descend(changes);

            addDefaults(changes);

            migrator.migrate(new SuperRootChangeImpl(changes));

            Map<String, Serializable> allChanges = createFlattenedUpdatesMap(
                    localRoots.rootsWithoutDefaults,
                    changes,
                    localRoots.storageData
            );

            if (onStartup) {
                for (String ignoredValue : ignoredKeys) {
                    allChanges.put(ignoredValue, null);
                }
            }

            dropUnnecessarilyDeletedKeys(allChanges, localRoots);

            if (allChanges.isEmpty() && onStartup) {
                // We don't want an empty storage update if this is the initialization changer.
                return nullCompletedFuture();
            }

            dropNulls(changes);

            validateConfiguration(curRoots, changes);

            // In some cases some storages may not want to persist configuration defaults.
            // Need to filter it from change map before write to storage.
            if (!storage.supportDefaults()) {
                removeDefaultValues(allChanges);
            }

            // "allChanges" map can be empty here in case the given update matches the current state of the local configuration. We
            // still try to write the empty update, because local configuration can be obsolete. If this is the case, then the CAS will
            // fail and the update will be recalculated and there is a chance that the new local configuration will produce a non-empty
            // update.
            return storage.write(allChanges, localRoots.changeId)
                    .thenCompose(casWroteSuccessfully -> {
                        if (casWroteSuccessfully) {
                            return localRoots.changeFuture;
                        } else {
                            // Here we go to next iteration of an implicit spin loop; we have to do it via recursion
                            // because we work with async code (futures).
                            return localRoots.changeFuture.thenCompose(v -> changeInternally(src, onStartup));
                        }
                    });
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void validateConfiguration(SuperRoot configuration) {
        List<ValidationIssue> validationIssues = configurationValidator.validate(configuration);

        if (!validationIssues.isEmpty()) {
            throw new ConfigurationValidationException(validationIssues);
        }
    }

    private void validateConfiguration(SuperRoot curRoots, SuperRoot changes) {
        List<ValidationIssue> validationIssues = configurationValidator.validate(curRoots, changes);

        if (!validationIssues.isEmpty()) {
            throw new ConfigurationValidationException(validationIssues);
        }
    }

    private ConfigurationStorageListener configurationStorageListener() {
        return changedEntries -> {
            StorageRoots oldStorageRoots = storageRoots;

            try {
                var changedValues = new HashMap<String, Serializable>(changedEntries.values());

                SuperRoot oldSuperRoot = oldStorageRoots.roots;
                SuperRoot oldSuperRootNoDefaults = oldStorageRoots.rootsWithoutDefaults;
                SuperRoot newSuperRoot = oldSuperRoot.copy();
                SuperRoot newSuperNoDefaults = oldSuperRootNoDefaults.copy();

                // We need to ignore deletion of deprecated values.
                ignoreDeleted(changedValues, keyIgnorer);

                Map<String, ?> dataValuesPrefixMap = toPrefixMap(changedValues);
                ignoreLegacyKeys(oldStorageRoots.roots, dataValuesPrefixMap);

                compressDeletedEntries(dataValuesPrefixMap);

                fillFromPrefixMap(newSuperRoot, dataValuesPrefixMap);
                fillFromPrefixMap(newSuperNoDefaults, dataValuesPrefixMap);

                long newChangeId = changedEntries.changeId();

                NavigableMap<String, ? extends Serializable> newData = mergeData(oldStorageRoots.storageData, changedEntries.values());
                var newStorageRoots = new StorageRoots(newSuperNoDefaults, newSuperRoot, newChangeId, newData);

                rwLock.writeLock().lock();

                try {
                    storageRoots = newStorageRoots;
                } finally {
                    rwLock.writeLock().unlock();
                }

                long notificationNumber = notificationListenerCnt.incrementAndGet();

                CompletableFuture<Void> notificationFuture = configurationUpdateListener
                        .onConfigurationUpdated(oldSuperRoot, newSuperRoot, newChangeId, notificationNumber);

                return notificationFuture.whenComplete((v, t) -> {
                    if (t == null) {
                        oldStorageRoots.changeFuture.complete(null);
                    } else {
                        oldStorageRoots.changeFuture.completeExceptionally(t);
                    }
                });
            } catch (Throwable e) {
                oldStorageRoots.changeFuture.completeExceptionally(e);

                return failedFuture(e);
            }
        };
    }

    private static NavigableMap<String, ? extends Serializable> mergeData(
            NavigableMap<String, ? extends Serializable> currentData,
            Map<String, ? extends Serializable> delta
    ) {
        NavigableMap<String, Serializable> newState = new TreeMap<>(currentData);

        for (Entry<String, ? extends Serializable> entry : delta.entrySet()) {
            if (entry.getValue() == null) {
                newState.remove(entry.getKey());
            } else {
                newState.put(entry.getKey(), entry.getValue());
            }
        }

        return newState;
    }

    /**
     * Remove keys from {@code allChanges}, that are associated with nulls in this map, and already absent in
     * {@link StorageRoots#storageData}.
     */
    private static void dropUnnecessarilyDeletedKeys(Map<String, Serializable> allChanges, StorageRoots localRoots) {
        allChanges.entrySet().removeIf(entry -> entry.getValue() == null && !localRoots.storageData.containsKey(entry.getKey()));
    }

    private void removeDefaultValues(Map<String, Serializable> allChanges) {
        defaultsMap.get().forEach((key, defaultValue) -> {
            Serializable change = allChanges.get(key);
            if (Objects.deepEquals(change, defaultValue)) {
                allChanges.put(key, null);
            }
        });
    }

    private Map<String, Serializable> createDefaultsMap() {
        SuperRoot superRoot = new SuperRoot(rootCreator());
        for (RootKey<?, ?, ?> rootKey : rootKeys.values()) {
            superRoot.addRoot(rootKey, createRootNode(rootKey));
        }

        SuperRoot defaults = superRoot.copy();
        addDefaults(defaults);

        return createFlattenedUpdatesMap(
                superRoot,
                defaults,
                emptyNavigableMap()
        );
    }

    /** {@inheritDoc} */
    @Override
    public long notificationCount() {
        return notificationListenerCnt.get();
    }

    private static <T> T get(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw new IgniteInternalException("Failed to read storage data", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInternalException("Failed to read storage data", e);
        }
    }
}
