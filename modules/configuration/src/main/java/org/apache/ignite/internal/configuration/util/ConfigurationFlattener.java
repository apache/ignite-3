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

package org.apache.ignite.internal.configuration.util;

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.KEY_SEPARATOR;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.escape;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.SortedMap;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.jetbrains.annotations.Nullable;

/** Utility class that has {@link ConfigurationFlattener#createFlattenedUpdatesMap} method. */
public class ConfigurationFlattener {
    /**
     * Convert a traversable tree to a map of qualified keys to values.
     *
     * @param curRoot Current root tree.
     * @param updates Tree with updates.
     * @return Map of changes.
     */
    public static Map<String, Serializable> createFlattenedUpdatesMap(
            SuperRoot curRoot,
            SuperRoot updates,
            NavigableMap<String, ? extends Serializable> storageData
    ) {
        // Resulting map.
        Map<String, Serializable> resMap = new HashMap<>();

        // This method traverses two trees at the same time. In order to reuse the visitor object it's decided to
        // use an explicit stack for nodes of the "old" tree. We need to reuse the visitor object because it accumulates
        // "full" keys in dot-separated notation. Please refer to KeysTrackingConfigurationVisitor for details..
        Deque<InnerNode> oldInnerNodesStack = new ArrayDeque<>();

        oldInnerNodesStack.push(curRoot);

        // Explicit access to the children of super root guarantees that "oldInnerNodesStack" is never empty, and thus
        // we don't need null-checks when calling Deque#peek().
        updates.traverseChildren(new FlattenerVisitor(oldInnerNodesStack, resMap, storageData), true);

        assert oldInnerNodesStack.peek() == curRoot : oldInnerNodesStack;

        return resMap;
    }

    /**
     * Returns map that contains same keys and their positions as values.
     *
     * @param node Named list node.
     * @return Map that contains same keys and their positions as values.
     */
    private static Map<String, Integer> keysToOrderIdx(NamedListNode<?> node) {
        Map<String, Integer> res = new HashMap<>();

        int idx = 0;

        for (String key : node.namedListKeys()) {
            if (node.getInnerNode(key) != null) {
                res.put(key, idx++);
            }
        }

        return res;
    }

    /**
     * Visitor that collects diff between "old" and "new" trees into a flat map.
     */
    private static class FlattenerVisitor extends KeysTrackingConfigurationVisitor<Object> {
        /** Old nodes stack for recursion. */
        private final Deque<InnerNode> oldInnerNodesStack;

        /** Map with the result. */
        private final Map<String, Serializable> resMap;

        /** Map with values in the configuration storage. */
        private final NavigableMap<String, ? extends Serializable> storageData;

        /** Flag indicates that "old" and "new" trees are literally the same at the moment. */
        private boolean singleTreeTraversal;

        /**
         * Makes sense only if {@link #singleTreeTraversal} is {@code true}. Helps distinguishing creation from deletion.
         * Always {@code false} if {@link #singleTreeTraversal} is {@code false}.
         */
        private boolean deletion;

        /**
         * Constructor.
         *
         * @param oldInnerNodesStack Old nodes stack for recursion.
         * @param resMap Map with the result.
         */
        FlattenerVisitor(
                Deque<InnerNode> oldInnerNodesStack,
                Map<String, Serializable> resMap,
                NavigableMap<String, ? extends Serializable> storageData
        ) {
            this.oldInnerNodesStack = oldInnerNodesStack;
            this.resMap = resMap;
            this.storageData = storageData;
        }

        private void putToMap(boolean mustOverride, boolean delete, Supplier<String> key, @Nullable Serializable newVal) {
            if (mustOverride) {
                // This branch does the unconditional update, because it is known that the value must be updated.
                resMap.put(key.get(), delete ? null : newVal);
            } else {
                String currentKey = key.get();

                // Here the value must not be updated, but it could be updated. This code corresponds to a scenario where node restarts on
                // a new version of Ignite and the name of the configuration key is changed to a new one. There's a separate piece of code
                // that deletes all usages of old name. This particular code makes sure that we re-write each value with their new keys.
                if (!storageData.containsKey(currentKey)) {
                    resMap.put(currentKey, newVal);
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        public Void doVisitLeafNode(Field field, String key, Serializable newVal) {
            // Read same value from old tree.
            Serializable oldVal = oldInnerNodesStack.element().traverseChild(key, ConfigurationUtil.leafNodeVisitor(), true);

            // Do not put duplicates into the resulting map.
            putToMap(singleTreeTraversal || !Objects.deepEquals(oldVal, newVal), deletion, this::currentKey, newVal);

            return null;
        }

        @Override
        protected Object doVisitLegacyLeafNode(Field field, String key, Serializable val, boolean isDeprecated) {
            String currentKey = currentKey();

            if (storageData.containsKey(currentKey)) {
                resMap.put(currentKey, null);
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override
        public Void doVisitInnerNode(Field field, String key, InnerNode newNode) {
            // Read same node from old tree.
            InnerNode oldNode = oldInnerNodesStack.element().traverseChild(key, ConfigurationUtil.innerNodeVisitor(), true);

            // In case inner node is null in both trees,
            // see LocalFileConfigurationStorageTest#innerNodeWithPartialContent – the node is someConfigurationValue.
            if (oldNode == null && newNode == null) {
                return null;
            }

            if (oldNode == null) {
                visitAsymmetricInnerNode(newNode, false);
            } else if (oldNode.schemaType() != newNode.schemaType()) {
                // At the moment, we do not separate the general fields from the fields of
                // specific instances of the polymorphic configuration, so we will assume
                // that all the fields have changed, perhaps we will fix this later.
                visitAsymmetricInnerNode(oldNode, true);

                visitAsymmetricInnerNode(newNode, false);
            } else {
                oldInnerNodesStack.push(oldNode);

                newNode.traverseChildren(this, true);

                oldInnerNodesStack.pop();
            }

            return null;
        }

        @Override
        protected Object doVisitLegacyInnerNode(Field field, String key, InnerNode node, boolean isDeprecated) {
            dropOutdatedData();

            return null;
        }

        /** {@inheritDoc} */
        @Override
        public Void doVisitNamedListNode(Field field, String key, NamedListNode<?> newNode) {
            // Read same named list node from old tree.
            NamedListNode<?> oldNode =
                    oldInnerNodesStack.element().traverseChild(key, ConfigurationUtil.namedListNodeVisitor(), true);

            // Old keys ordering can be ignored if we either create or delete everything.
            Map<String, Integer> oldKeysToOrderIdxMap = singleTreeTraversal ? null : keysToOrderIdx(oldNode);

            // New keys ordering can be ignored if we delete everything.
            Map<String, Integer> newKeysToOrderIdxMap = deletion ? null : keysToOrderIdx(newNode);

            for (String newNodeKey : newNode.namedListKeys()) {
                UUID newNodeInternalId = newNode.internalId(newNodeKey);

                String namedListFullKey = currentKey();

                withTracking(field, newNodeInternalId.toString(), false, false, () -> {
                    InnerNode newNamedElement = newNode.getInnerNode(newNodeKey);

                    String oldNodeKey = oldNode.keyByInternalId(newNodeInternalId);
                    InnerNode oldNamedElement = oldNode.getInnerNode(oldNodeKey);

                    // Deletion of nonexistent element.
                    if (oldNamedElement == null && newNamedElement == null) {
                        return null;
                    }

                    if (newNamedElement == null) {
                        visitAsymmetricInnerNode(oldNamedElement, true);
                    } else if (oldNamedElement == null) {
                        visitAsymmetricInnerNode(newNamedElement, false);
                    } else if (newNamedElement.schemaType() != oldNamedElement.schemaType()) {
                        // At the moment, we do not separate the general fields from the fields of
                        // specific instances of the polymorphic configuration, so we will assume
                        // that all the fields have changed, perhaps we will fix this later.
                        visitAsymmetricInnerNode(oldNamedElement, true);

                        visitAsymmetricInnerNode(newNamedElement, false);
                    } else {
                        oldInnerNodesStack.push(oldNamedElement);

                        newNamedElement.traverseChildren(this, true);

                        oldInnerNodesStack.pop();
                    }

                    Integer newIdx = newKeysToOrderIdxMap == null ? null : newKeysToOrderIdxMap.get(newNodeKey);
                    Integer oldIdx = oldKeysToOrderIdxMap == null ? null : oldKeysToOrderIdxMap.get(newNodeKey);

                    // We should "persist" changed indexes only.
                    putToMap(
                            !Objects.equals(newIdx, oldIdx) || singleTreeTraversal || newNamedElement == null,
                            deletion || newNamedElement == null,
                            () -> currentKey() + NamedListNode.ORDER_IDX,
                            newIdx
                    );

                    // If it's creation / deletion / rename.
                    putToMap(
                            singleTreeTraversal || oldNamedElement == null || newNamedElement == null || !oldNodeKey.equals(newNodeKey),
                            deletion || newNamedElement == null,
                            () -> currentKey() + NamedListNode.NAME,
                            newNodeKey
                    );

                    if (singleTreeTraversal) {
                        if (deletion) {
                            // Deletion as a part of outer named list element.
                            resMap.put(idKey(namedListFullKey, oldNodeKey), null);
                        } else {
                            // Creation as a part of outer named list's new element.
                            resMap.put(idKey(namedListFullKey, newNodeKey), newNodeInternalId);
                        }
                    } else {
                        // Regular deletion.
                        if (newNamedElement == null) {
                            resMap.put(idKey(namedListFullKey, oldNodeKey), null);
                        } else if (oldNamedElement == null) {
                            // Regular creation.
                            resMap.put(idKey(namedListFullKey, newNodeKey), newNodeInternalId);
                        } else if (!oldNodeKey.equals(newNodeKey)) {
                            // Rename. Old value is nullified.
                            resMap.put(idKey(namedListFullKey, oldNodeKey), null);

                            // And new value is initialized.
                            resMap.put(idKey(namedListFullKey, newNodeKey), newNodeInternalId);
                        }
                    }

                    // Don't use "putToMap" method here because it would be too complicated due to all the conditions above.
                    String idKey = idKey(namedListFullKey, newNodeKey);
                    if (!storageData.containsKey(idKey)) {
                        resMap.put(idKey, newNodeInternalId);
                    }

                    return null;
                });
            }

            return null;
        }

        @Override
        protected Object doVisitLegacyNamedListNode(Field field, String key, NamedListNode<?> node, boolean isDeprecated) {
            dropOutdatedData();

            return null;
        }

        private void dropOutdatedData() {
            String currentKey = currentKey();
            SortedMap<String, ? extends Serializable> tailMap = storageData.tailMap(currentKey);

            for (String storageKey : tailMap.keySet()) {
                if (!storageKey.startsWith(currentKey)) {
                    break;
                }

                resMap.put(storageKey, null);
            }
        }

        /**
         * Creates key {@code prefix.<ids>.nodeKey}, escaping {@code nodeKey} before appending it.
         */
        private static String idKey(String prefix, String nodeKey) {
            return prefix + NamedListNode.IDS + KEY_SEPARATOR + escape(nodeKey);
        }

        /**
         * Here we must list all joined keys belonging to deleted or created element. The only way to do it is to traverse the entire
         * configuration tree unconditionally.
         */
        private void visitAsymmetricInnerNode(InnerNode node, boolean delete) {
            assert node != null;

            oldInnerNodesStack.push(node);
            singleTreeTraversal = true;
            deletion = delete;

            node.traverseChildren(this, true);

            deletion = false;
            singleTreeTraversal = false;
            oldInnerNodesStack.pop();
        }
    }
}
