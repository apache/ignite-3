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
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.internal.util.KeysTrackingConfigurationVisitor;
import org.apache.ignite.configuration.internal.validation.MemberKey;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.NamedListNode;
import org.apache.ignite.configuration.tree.TraversableTreeNode;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

import static java.util.Collections.emptySet;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.fillFromPrefixMap;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.find;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.nodeToFlatMap;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.patch;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.toPrefixMap;

/**
 * Class that handles configuration changes, by validating them, passing to storage and listening to storage updates.
 */
public class ConfigurationChanger {
    /** */
    private final ForkJoinPool pool = new ForkJoinPool(2);

    /** */
    private final Set<RootKey<?, ?>> rootKeys = new HashSet<>();

    /** Map that has all the trees in accordance to their storages. */
    private final Map<Class<? extends ConfigurationStorage>, StorageRoots> storagesRootsMap = new ConcurrentHashMap<>();

    /** Annotation classes mapped to validator objects. */
    private Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators = new HashMap<>();

    /**
     * Immutable data container to store version and all roots associated with the specific storage.
     */
    public static class StorageRoots {
        /** Immutable forest, so to say. */
        private final Map<RootKey<?, ?>, InnerNode> roots;

        /** Version associated with the currently known storage state. */
        private final long version;

        /** */
        private StorageRoots(Map<RootKey<?, ?>, InnerNode> roots, long version) {
            this.roots = Collections.unmodifiableMap(roots);
            this.version = version;
        }
    }

    /** Lazy annotations cache for configuration schema fields. */
    private final Map<MemberKey, Set<Annotation>> cachedAnnotations = new ConcurrentHashMap<>();

    /** Storage instances by their classes. Comes in handy when all you have is {@link RootKey}. */
    private final Map<Class<? extends ConfigurationStorage>, ConfigurationStorage> storageInstances = new HashMap<>();

    /** Constructor. */
    public ConfigurationChanger(RootKey<?, ?>... rootKeys) {
        this.rootKeys.addAll(Arrays.asList(rootKeys));
    }

    /** */
    public <A extends Annotation> void addValidator(Class<A> annotationType, Validator<A, ?> validator) {
        validators
            .computeIfAbsent(annotationType, a -> new HashSet<>())
            .add(validator);
    }

    /** */
    public void addRootKey(RootKey<?, ?> rootKey) {
        assert !storageInstances.containsKey(rootKey.getStorageType());

        rootKeys.add(rootKey);
    }

    /**
     * Initialize changer.
     */
    // ConfigurationChangeException, really?
    public void init(ConfigurationStorage configurationStorage) throws ConfigurationChangeException {
        storageInstances.put(configurationStorage.getClass(), configurationStorage);

        Set<RootKey<?, ?>> storageRootKeys = rootKeys.stream().filter(
            rootKey -> configurationStorage.getClass() == rootKey.getStorageType()
        ).collect(Collectors.toSet());

        Data data;

        try {
            data = configurationStorage.readAll();
        }
        catch (StorageException e) {
            throw new ConfigurationChangeException("Failed to initialize configuration: " + e.getMessage(), e);
        }

        Map<RootKey<?, ?>, InnerNode> storageRootsMap = new HashMap<>();
        // Map to collect defaults for not initialized configurations.
        Map<RootKey<?, ?>, InnerNode> storageDefaultsMap = new HashMap<>();

        Map<String, ?> dataValuesPrefixMap = toPrefixMap(data.values());

        for (RootKey<?, ?> rootKey : storageRootKeys) {
            Map<String, ?> rootPrefixMap = (Map<String, ?>)dataValuesPrefixMap.get(rootKey.key());

            InnerNode rootNode = rootKey.createRootNode();

            if (rootPrefixMap != null)
                fillFromPrefixMap(rootNode, rootPrefixMap);

            // Collecting defaults requires fresh new root.
            InnerNode defaultsNode = rootKey.createRootNode();

            addDefaults(rootNode, defaultsNode);

            storageRootsMap.put(rootKey, rootNode);
            storageDefaultsMap.put(rootKey, defaultsNode);
        }

        StorageRoots storageRoots = new StorageRoots(storageRootsMap, data.version());

        storagesRootsMap.put(configurationStorage.getClass(), storageRoots);

        configurationStorage.addListener(changedEntries -> updateFromListener(
            configurationStorage.getClass(),
            changedEntries
        ));

        // Do this strictly after adding listeners, otherwise we can lose these changes.
        try {
            change(storageDefaultsMap).get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new ConfigurationChangeException(
                "Failed to write defalut configuration values into the storage " + configurationStorage.getClass(), e
            );
        }
    }

    /**
     * Fill {@code dst} node with default values, required to complete {@code src} node.
     * These two objects can be the same, this would mean that all {@code null} values of {@code scr} will be
     * replaced with defaults if it's possible.
     *
     * @param src Source node.
     * @param dst Destination node.
     */
    private void addDefaults(InnerNode src, InnerNode dst) {
        src.traverseChildren(new ConfigurationVisitor<>() {
            @Override public Object visitLeafNode(String key, Serializable val) {
                // If source value is null then inititalise the same value on the destination node.
                if (val == null)
                    dst.constructDefault(key);

                return null;
            }

            @Override public Object visitInnerNode(String key, InnerNode srcNode) {
                // Instantiate field in destination node before doing something else.
                // Not a big deal if it wasn't null.
                dst.construct(key, new ConfigurationSource() {});

                // Get that inner node from destination to continue the processing.
                InnerNode dstNode = dst.traverseChild(key, new ConfigurationVisitor<>() {
                    @Override public InnerNode visitInnerNode(String key, InnerNode dstNode) {
                        return dstNode;
                    }
                });

                // "dstNode" is guaranteed to not be null even if "src" and "dst" match.
                // Null in "srcNode" means that we should initialize everything that we can in "dstNode"
                // unconditionally. It's only possible if we pass it as a source as well.
                addDefaults(srcNode == null ? dstNode : srcNode, dstNode);

                return null;
            }

            @Override public <N extends InnerNode> Object visitNamedListNode(String key, NamedListNode<N> srcNamedList) {
                // Here we don't need to preemptively initialise corresponsing field, because it can never be null.
                NamedListNode<?> dstNamedList = dst.traverseChild(key, new ConfigurationVisitor<>() {
                    @Override public <N extends InnerNode> NamedListNode<?> visitNamedListNode(String key, NamedListNode<N> dstNode) {
                        return dstNode;
                    }
                });

                for (String namedListKey : srcNamedList.namedListKeys()) {
                    // But, in order to get non-null value from "dstNamedList.get(namedListKey)" we must explicitly
                    // ensure its existance.
                    dstNamedList.construct(namedListKey, new ConfigurationSource() {});

                    addDefaults(srcNamedList.get(namedListKey), dstNamedList.get(namedListKey));
                }

                return null;
            }
        });
    }

    /**
     * Get root node by root key. Subject to revisiting.
     *
     * @param rootKey Root key.
     */
    public TraversableTreeNode getRootNode(RootKey<?, ?> rootKey) {
        return this.storagesRootsMap.get(rootKey.getStorageType()).roots.get(rootKey);
    }

    /**
     * Change configuration.
     * @param changes Map of changes by root key.
     */
    public CompletableFuture<Void> change(Map<RootKey<?, ?>, ? extends TraversableTreeNode> changes) {
        if (changes.isEmpty())
            return CompletableFuture.completedFuture(null);

        Set<Class<? extends ConfigurationStorage>> storagesTypes = changes.keySet().stream()
            .map(RootKey::getStorageType)
            .collect(Collectors.toSet());

        assert !storagesTypes.isEmpty();

        if (storagesTypes.size() != 1) {
            return CompletableFuture.failedFuture(
                new ConfigurationChangeException("Cannot change configurations belonging to different storages.")
            );
        }

        Class<? extends ConfigurationStorage> storageType = storagesTypes.iterator().next();

        ConfigurationStorage storage = storageInstances.get(storageType);

        CompletableFuture<Void> fut = new CompletableFuture<>();

        pool.execute(() -> change0(changes, storage, fut));

        return fut;
    }

    /**
     * Internal configuration change method that completes provided future.
     * @param changes Map of changes by root key.
     * @param storage Storage instance.
     * @param fut Future, that must be completed after changes are written to the storage.
     */
    private void change0(
        Map<RootKey<?, ?>, ? extends TraversableTreeNode> changes,
        ConfigurationStorage storage,
        CompletableFuture<?> fut
    ) {
        StorageRoots storageRoots = storagesRootsMap.get(storage.getClass());

        Map<RootKey<?, ?>, InnerNode> rootsForValidation = new HashMap<>();

        Map<String, Serializable> allChanges = new HashMap<>();

        for (Map.Entry<RootKey<?, ?>, ? extends TraversableTreeNode> entry : changes.entrySet()) {
            RootKey<?, ?> rootKey = entry.getKey();
            TraversableTreeNode change = entry.getValue();

            // It's important to get the root from "roots" object rather then "storageRootMap" or "getRootNode(...)".
            InnerNode currentRootNode = storageRoots.roots.get(rootKey);

            //TODO single putAll + remove matching value, this way "allChanges" will be fair.
            // These are changes explicitly provided by the client.
            allChanges.putAll(nodeToFlatMap(rootKey, currentRootNode, change));

            // It is necessary to reinitialize default values every time.
            // Possible use case that explicitly requires it: creation of the same named list entry with slightly
            // different set of values and different dynamic defaults at the same time.
            InnerNode patchedRootNode = patch(currentRootNode, change);
            InnerNode defaultsNode = rootKey.createRootNode();

            addDefaults(patchedRootNode, defaultsNode);

            // These are default values for non-initialized values, required to complete the configuration.
            allChanges.putAll(nodeToFlatMap(rootKey, patchedRootNode, defaultsNode));

            rootsForValidation.put(rootKey, patch(patchedRootNode, defaultsNode));
        }

        // Unlikely but still possible.
        if (allChanges.isEmpty()) {
            fut.complete(null);

            return;
        }

        ValidationResult validationResult = validate(storageRoots, rootsForValidation);

        List<ValidationIssue> validationIssues = validationResult.issues();

        if (!validationIssues.isEmpty()) {
            fut.completeExceptionally(new ConfigurationValidationException(validationIssues));

            return;
        }

        CompletableFuture<Boolean> writeFut = storage.write(allChanges, storageRoots.version);

        writeFut.whenCompleteAsync((casResult, throwable) -> {
            if (throwable != null)
                fut.completeExceptionally(new ConfigurationChangeException("Failed to change configuration", throwable));
            else if (casResult)
                fut.complete(null);
            else
                change0(changes, storage, fut);
        }, pool);
    }

    /**
     * Update configuration from storage listener.
     * @param storageType Type of the storage that propagated these changes.
     * @param changedEntries Changed data.
     */
    private void updateFromListener(
        Class<? extends ConfigurationStorage> storageType,
        Data changedEntries
    ) {
        StorageRoots oldStorageRoots = this.storagesRootsMap.get(storageType);

        Map<RootKey<?, ?>, InnerNode> storageRootsMap = new HashMap<>(oldStorageRoots.roots);

        Map<String, ?> dataValuesPrefixMap = toPrefixMap(changedEntries.values());

        compressDeletedEntries(dataValuesPrefixMap);

        for (RootKey<?, ?> rootKey : oldStorageRoots.roots.keySet()) {
            Map<String, ?> rootPrefixMap = (Map<String, ?>)dataValuesPrefixMap.get(rootKey.key());

            if (rootPrefixMap != null) {
                InnerNode rootNode = oldStorageRoots.roots.get(rootKey).copy();

                fillFromPrefixMap(rootNode, rootPrefixMap);

                storageRootsMap.put(rootKey, rootNode);
            }
        }

        StorageRoots storageRoots = new StorageRoots(storageRootsMap, changedEntries.version());

        storagesRootsMap.put(storageType, storageRoots);

        //TODO IGNITE-14180 Notify listeners.
    }

    /**
     * "Compress" prefix map - this means that deleted named list elements will be represented as a single {@code null}
     * objects instead of a number of nullified configuration leaves.
     *
     * @param prefixMap Prefix map, constructed from the storage notification data or its subtree.
     */
    private void compressDeletedEntries(Map<String, ?> prefixMap) {
        // Here we basically assume that if prefix subtree contains single null child then all its childrens are nulls.
        Set<String> keysForRemoval = prefixMap.entrySet().stream()
            .filter(entry ->
                entry.getValue() instanceof Map && ((Map<?, ?>)entry.getValue()).containsValue(null)
            )
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

        // Replace all such elements will nulls, signifying that these are deleted named list elements.
        for (String key : keysForRemoval)
            prefixMap.put(key, null);

        // Continue recursively.
        for (Object value : prefixMap.values()) {
            if (value instanceof Map)
                compressDeletedEntries((Map<String, ?>)value);
        }
    }

    /**
     * Validate configuration changes.
     *
     * @param storageRoots Storage roots.
     * @param newRoots Configuration changes.
     * @return Validation results.
     */
    private ValidationResult validate(
        StorageRoots storageRoots,
        Map<RootKey<?, ?>, ? extends TraversableTreeNode> newRoots
    ) {
        List<ValidationIssue> issues = new ArrayList<>();

        for (Map.Entry<RootKey<?, ?>, ? extends TraversableTreeNode> entry : newRoots.entrySet()) {
            RootKey<?, ?> rootKey = entry.getKey();
            TraversableTreeNode newRoot = entry.getValue();

            newRoot.accept(rootKey.key(), new KeysTrackingConfigurationVisitor<>() {
                /** Inner nodes, last one always belongs to "current" leaf. */
                private Deque<InnerNode> innerNodes = new ArrayDeque<>();

                /** {@inheritDoc} */
                @Override protected Object visitInnerNode0(String key, InnerNode node) {
                    assert node != null;

                    // There cannot be validation annotations on the root itself. Condition is correct.
                    if (!innerNodes.isEmpty()) {
                        String currentKey = currentKey();

                        // Last dot should be trimmed.
                        validate(innerNodes.peek(), key, node, currentKey.substring(0, currentKey.length() - 1));
                    }

                    innerNodes.push(node);

                    try {
                        return super.visitInnerNode0(key, node);
                    }
                    finally {
                        innerNodes.pop();
                    }
                }

                /** {@inheritDoc} */
                @Override protected Void visitLeafNode0(String key, Serializable val) {
                    if (val == null) {
                        String message = "'" + currentKey() + "' configuration value is not initialized.";

                        issues.add(new ValidationIssue(message));
                    }
                    else
                        validate(innerNodes.peek(), key, val, currentKey());

                    return null;
                }

                /**
                 * Perform validation on the node's subnode.
                 *
                 * @param lastInnerNode Inner node that contains validated field.
                 * @param fieldName Name of the field.
                 * @param val Value of the field.
                 * @param currentKey Fully qualified key for the field.
                 */
                private void validate(InnerNode lastInnerNode, String fieldName, Object val, String currentKey) {
                    MemberKey memberKey = new MemberKey(lastInnerNode.getClass(), fieldName);

                    Set<Annotation> annotations = cachedAnnotations.computeIfAbsent(memberKey, k -> {
                        try {
                            Field field = lastInnerNode.schemaType().getDeclaredField(fieldName);

                            return Arrays.stream(field.getDeclaredAnnotations()).collect(Collectors.toSet());
                        }
                        catch (NoSuchFieldException e) {
                            // Should be impossible.
                            return emptySet();
                        }
                    });

                    for (Annotation annotation : annotations) {
                        for (Validator<?, ?> validatorX : validators.getOrDefault(annotation.annotationType(), emptySet())) {
                            // Making this a compile-time check would be too expensive to implement.
                            assert asserts(validatorX.getClass(), annotation.annotationType(), val);

                            Validator<Annotation, Object> validator = (Validator<Annotation, Object>)validatorX;

                            validator.validate(annotation, new ValidationContextImpl<>(storageRoots, rootKey, val, newRoots, issues, currentKey, currentPath()));
                        }
                    }
                }
            });
        }

        return new ValidationResult(issues);
    }

    /** */
    private static boolean asserts(Class<?> validatorClass, Class<? extends Annotation> annotationType, Object val) {
        if (!Arrays.asList(validatorClass.getInterfaces()).contains(Validator.class))
            return asserts(validatorClass.getSuperclass(), annotationType, val);

        Type genericSuperClass = Arrays.stream(validatorClass.getGenericInterfaces())
            .filter(i -> i instanceof ParameterizedType && ((ParameterizedType)i).getRawType() == Validator.class)
            .findAny()
            .get();

        assert genericSuperClass instanceof ParameterizedType;

        ParameterizedType parameterizedSuperClass = (ParameterizedType)genericSuperClass;

        Type[] actualTypeParameters = parameterizedSuperClass.getActualTypeArguments();

        assert actualTypeParameters.length == 2;

        assert actualTypeParameters[0] == annotationType;

        assert actualTypeParameters[1] instanceof Class;

        assert val == null || ((Class<?>)actualTypeParameters[1]).isInstance(val);

        return true;
    }

    /**
     * Results of the validation.
     */
    private static final class ValidationResult {
        /** List of issues. */
        private final List<ValidationIssue> issues;

        /**
         * Constructor.
         * @param issues List of issues.
         */
        private ValidationResult(List<ValidationIssue> issues) {
            this.issues = issues;
        }

        /**
         * Get issues.
         * @return Issues.
         */
        public List<ValidationIssue> issues() {
            return Collections.unmodifiableList(issues);
        }
    }

    /** */
    private class ValidationContextImpl<VIEW> implements ValidationContext<VIEW> {
        /** */
        private final StorageRoots storageRoots;

        /** */
        private final RootKey<?, ?> rootKey;

        /** */
        private final VIEW val;

        /** */
        private final Map<RootKey<?, ?>, ? extends TraversableTreeNode> newRoots;

        /** */
        private final List<ValidationIssue> issues;

        /** */
        private final String currentKey;

        /** */
        private final List<String> currentPath;

        ValidationContextImpl(StorageRoots storageRoots, RootKey<?, ?> rootKey, VIEW val,
            Map<RootKey<?, ?>, ? extends TraversableTreeNode> newRoots, List<ValidationIssue> issues,
            String currentKey, List<String> currentPath
        ) {
            this.storageRoots = storageRoots;
            this.rootKey = rootKey;
            this.val = val;
            this.newRoots = newRoots;
            this.issues = issues;
            this.currentKey = currentKey;
            this.currentPath = currentPath;
        }

        /** {@inheritDoc} */
        @Override public String currentKey() {
            return currentKey;
        }

        /** {@inheritDoc} */
        @Override public VIEW getOldValue() {
            return (VIEW)find(currentPath, storageRoots.roots.get(rootKey));
        }

        /** {@inheritDoc} */
        @Override public VIEW getNewValue() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public <ROOT> ROOT getOldRoot(RootKey<?, ROOT> rootKey) {
            InnerNode root = storageRoots.roots.get(rootKey);

            return (ROOT)(root == null ? getRootNode(rootKey) : root);
        }

        /** {@inheritDoc} */
        @Override public <ROOT> ROOT getNewRoot(RootKey<?, ROOT> rootKey) {
            TraversableTreeNode root = newRoots.get(rootKey);

            return (ROOT)(root == null ? getRootNode(rootKey) : root);
        }

        /** {@inheritDoc} */
        @Override public void addIssue(ValidationIssue issue) {
            issues.add(issue);
        }
    }
}
