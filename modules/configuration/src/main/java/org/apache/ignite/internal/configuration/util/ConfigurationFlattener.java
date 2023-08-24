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
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;

/** Utility class that has {@link ConfigurationFlattener#createFlattenedUpdatesMap(SuperRoot, SuperRoot)} method. */
public class ConfigurationFlattener {
    /**
     * Convert a traversable tree to a map of qualified keys to values.
     *
     * @param curRoot Current root tree.
     * @param updates Tree with updates.
     * @return Map of changes.
     */
    public static Map<String, Serializable> createFlattenedUpdatesMap(SuperRoot curRoot, SuperRoot updates) {
        // Resulting map.
        Map<String, Serializable> resMap = new HashMap<>();

        // This method traverses two trees at the same time. In order to reuse the visitor object it's decided to
        // use an explicit stack for nodes of the "old" tree. We need to reuse the visitor object because it accumulates
        // "full" keys in dot-separated notation. Please refer to KeysTrackingConfigurationVisitor for details..
        Deque<InnerNode> oldInnerNodesStack = new ArrayDeque<>();

        oldInnerNodesStack.push(curRoot);

        // Explicit access to the children of super root guarantees that "oldInnerNodesStack" is never empty, and thus
        // we don't need null-checks when calling Deque#peek().
        updates.traverseChildren(new FlattenerVisitor(oldInnerNodesStack, resMap), true);

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
        FlattenerVisitor(Deque<InnerNode> oldInnerNodesStack, Map<String, Serializable> resMap) {
            this.oldInnerNodesStack = oldInnerNodesStack;
            this.resMap = resMap;
        }

        /** {@inheritDoc} */
        @Override
        public Void doVisitLeafNode(Field field, String key, Serializable newVal) {
            // Read same value from old tree.
            Serializable oldVal = oldInnerNodesStack.element().traverseChild(key, ConfigurationUtil.leafNodeVisitor(), true);

            // Do not put duplicates into the resulting map.
            if (singleTreeTraversal || !Objects.deepEquals(oldVal, newVal)) {
                resMap.put(currentKey(), deletion ? null : newVal);
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override
        public Void doVisitInnerNode(Field field, String key, InnerNode newNode) {
            // Read same node from old tree.
            InnerNode oldNode = oldInnerNodesStack.element().traverseChild(key, ConfigurationUtil.innerNodeVisitor(), true);

            // Skip subtree that has not changed.
            if (oldNode == newNode && !singleTreeTraversal) {
                return null;
            }

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

        /** {@inheritDoc} */
        @Override
        public Void doVisitNamedListNode(Field field, String key, NamedListNode<?> newNode) {
            // Read same named list node from old tree.
            NamedListNode<?> oldNode =
                    oldInnerNodesStack.element().traverseChild(key, ConfigurationUtil.namedListNodeVisitor(), true);

            // Skip subtree that has not changed.
            if (oldNode == newNode && !singleTreeTraversal) {
                return null;
            }

            // Old keys ordering can be ignored if we either create or delete everything.
            Map<String, Integer> oldKeysToOrderIdxMap = singleTreeTraversal ? null
                    : keysToOrderIdx(oldNode);

            // New keys ordering can be ignored if we delete everything.
            Map<String, Integer> newKeysToOrderIdxMap = deletion ? null : keysToOrderIdx(newNode);

            for (String newNodeKey : newNode.namedListKeys()) {
                UUID newNodeInternalId = newNode.internalId(newNodeKey);

                String namedListFullKey = currentKey();

                withTracking(newNodeInternalId.toString(), false, false, () -> {
                    InnerNode newNamedElement = newNode.getInnerNode(newNodeKey);

                    String oldNodeKey = oldNode.keyByInternalId(newNodeInternalId);
                    InnerNode oldNamedElement = oldNode.getInnerNode(oldNodeKey);

                    // Deletion of nonexistent element.
                    if (oldNamedElement == null && newNamedElement == null) {
                        return null;
                    }

                    // Skip element that has not changed.
                    // Its index can be different though, so we don't "continue" straight away.
                    if (singleTreeTraversal || oldNamedElement != newNamedElement) {
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
                    }

                    Integer newIdx = newKeysToOrderIdxMap == null ? null : newKeysToOrderIdxMap.get(newNodeKey);
                    Integer oldIdx = oldKeysToOrderIdxMap == null ? null : oldKeysToOrderIdxMap.get(newNodeKey);

                    // We should "persist" changed indexes only.
                    if (!Objects.equals(newIdx, oldIdx) || singleTreeTraversal || newNamedElement == null) {
                        String orderKey = currentKey() + NamedListNode.ORDER_IDX;

                        resMap.put(orderKey, deletion || newNamedElement == null ? null : newIdx);
                    }

                    // If it's creation / deletion / rename.
                    if (singleTreeTraversal || oldNamedElement == null || newNamedElement == null
                            || !oldNodeKey.equals(newNodeKey)
                    ) {
                        String nameKey = currentKey() + NamedListNode.NAME;

                        resMap.put(nameKey, deletion || newNamedElement == null ? null : newNodeKey);
                    }

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

                    return null;
                });
            }

            return null;
        }

        /**
         * Creates key {@code prefix.<ids>.nodeKey}, escaping {@code nodeKey} before appending it.
         */
        private String idKey(String prefix, String nodeKey) {
            return prefix + NamedListNode.IDS + KEY_SEPARATOR + escape(nodeKey);
        }

        /**
         * Here we must list all joined keys belonging to deleted or created element. The only way to do it is to traverse the entire
         * configuration tree unconditionally.
         */
        private void visitAsymmetricInnerNode(InnerNode node, boolean delete) {
            assert !singleTreeTraversal;
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
