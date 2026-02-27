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

import static org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator.legacyNames;
import static org.apache.ignite.internal.util.ArrayUtils.STRING_EMPTY_ARRAY;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;

/** Visitor that accumulates keys and legacy names while descending. */
public abstract class KeysTrackingConfigurationVisitor<T> implements ConfigurationVisitor<T> {
    /** Current key, aggregated by visitor. */
    private final StringBuilder currentKey = new StringBuilder();

    /** Current keys list, almost the same as {@link #currentKey}. */
    private final List<String> currentPath = new ArrayList<>();

    private static boolean isDeprecated(Field field) {
        return field != null && field.getAnnotation(Deprecated.class) != null;
    }

    private static String[] getLegacyNames(Field field) {
        return field == null ? STRING_EMPTY_ARRAY : legacyNames(field);
    }

    /** {@inheritDoc} */
    @Override
    public final T visitLeafNode(Field field, String key, Serializable val) {
        for (String legacyKey : getLegacyNames(field)) {
            int prevPos = startVisit(legacyKey, false, true);

            try {
                doVisitLegacyLeafNode(field, legacyKey, val, false);
            } finally {
                endVisit(prevPos);
            }
        }

        int prevPos = startVisit(key, false, true);

        try {
            return isDeprecated(field) ? doVisitLegacyLeafNode(field, key, val, true) : doVisitLeafNode(field, key, val);
        } finally {
            endVisit(prevPos);
        }
    }

    /** {@inheritDoc} */
    @Override
    public final T visitInnerNode(Field field, String key, InnerNode node) {
        for (String legacyKey : getLegacyNames(field)) {
            int prevPos = startVisit(legacyKey, false, false);

            try {
                doVisitLegacyInnerNode(field, legacyKey, node, false);
            } finally {
                endVisit(prevPos);
            }
        }

        int prevPos = startVisit(key, false, false);

        try {
            return isDeprecated(field) ? doVisitLegacyInnerNode(field, key, node, true) : doVisitInnerNode(field, key, node);
        } finally {
            endVisit(prevPos);
        }
    }

    /** {@inheritDoc} */
    @Override
    public final T visitNamedListNode(Field field, String key, NamedListNode<?> node) {
        for (String legacyKey : getLegacyNames(field)) {
            int prevPos = startVisit(legacyKey, false, false);

            try {
                doVisitLegacyNamedListNode(field, legacyKey, node, false);
            } finally {
                endVisit(prevPos);
            }
        }

        int prevPos = startVisit(key, false, false);

        try {
            return isDeprecated(field) ? doVisitLegacyNamedListNode(field, key, node, true) : doVisitNamedListNode(field, key, node);
        } finally {
            endVisit(prevPos);
        }
    }

    /**
     * To be used instead of {@link ConfigurationVisitor#visitLeafNode(Field, String, Serializable)}.
     *
     * @param key Name of the node retrieved from its holder object.
     * @param val Configuration value.
     * @return Anything that implementation decides to return.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected T doVisitLeafNode(Field field, String key, Serializable val) {
        return null;
    }

    /**
     * Almost the same as {@link #doVisitLeafNode(Field, String, Serializable)}, but used for legacy fields.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected T doVisitLegacyLeafNode(Field field, String key, Serializable val, boolean isDeprecated) {
        return null;
    }

    /**
     * To be used instead of {@link ConfigurationVisitor#visitInnerNode(Field, String, InnerNode)}.
     *
     * @param key  Name of the node retrieved from its holder object.
     * @param node Inner configuration node.
     * @return Anything that implementation decides to return.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected T doVisitInnerNode(Field field, String key, InnerNode node) {
        node.traverseChildren(this, true);

        return null;
    }

    /**
     * Almost the same as {@link #doVisitInnerNode(Field, String, InnerNode)}, but used for legacy fields.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected T doVisitLegacyInnerNode(Field field, String key, InnerNode node, boolean isDeprecated) {
        return null;
    }

    /**
     * To be used instead of {@link ConfigurationVisitor#visitNamedListNode}}.
     *
     * @param key  Name of the node retrieved from its holder object.
     * @param node Named list inner configuration node.
     * @return Anything that implementation decides to return.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected T doVisitNamedListNode(Field field, String key, NamedListNode<?> node) {
        for (String namedListKey : node.namedListKeys()) {
            int prevPos = startVisit(namedListKey, true, false);

            try {
                doVisitInnerNode(field, namedListKey, node.getInnerNode(namedListKey));
            } finally {
                endVisit(prevPos);
            }
        }

        return null;
    }

    /**
     * Almost the same as {@link #doVisitNamedListNode(Field, String, NamedListNode)}, but used for legacy fields.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected T doVisitLegacyNamedListNode(Field field, String key, NamedListNode<?> node, boolean isDeprecated) {
        return null;
    }

    /**
     * Tracks passed key to reflect it in {@link #currentKey()} and {@link #currentPath()}.
     *
     * @param field Node's field.
     * @param key Key itself.
     * @param escape Whether the key needs escaping or not.
     * @param leaf Add dot at the end of {@link #currentKey()} if {@code leaf} is {@code false}.
     * @param closure Closure to execute when {@link #currentKey()} and {@link #currentPath()} have updated values.
     * @return Closure result.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected final T withTracking(Field field, String key, boolean escape, boolean leaf, Supplier<T> closure) {
        int prevPos = startVisit(key, escape, leaf);

        try {
            return closure.get();
        } finally {
            endVisit(prevPos);
        }
    }

    /**
     * Returns current key, with a dot at the end if it's not a leaf.
     *
     * @return Current key, with a dot at the end if it's not a leaf.
     */
    protected final String currentKey() {
        return currentKey.toString();
    }

    /**
     * Returns list representation of the current key.
     *
     * @return List representation of the current key.
     */
    protected final List<String> currentPath() {
        return Collections.unmodifiableList(currentPath);
    }

    /**
     * Prepares values of {@link #currentKey} and {@link #currentPath} for further processing.
     *
     * @param key Key.
     * @param escape Whether we need to escape the key before appending it to {@link #currentKey}.
     * @param leaf Add dot at the end of {@link #currentKey()} if {@code leaf} is {@code false}.
     * @return Previous length of {@link #currentKey} so it can be passed to {@link #endVisit(int)} later.
     */
    private int startVisit(String key, boolean escape, boolean leaf) {
        final int previousKeyLength = currentKey.length();

        currentKey.append(escape ? ConfigurationUtil.escape(key) : key);

        if (!leaf) {
            currentKey.append('.');
        }

        currentPath.add(key);

        return previousKeyLength;
    }

    /**
     * Puts {@link #currentKey} and {@link #currentPath} in the same state as they were before {@link #startVisit}.
     *
     * @param previousKeyLength Value return by corresponding {@link #startVisit} invocation.
     */
    private void endVisit(int previousKeyLength) {
        currentKey.setLength(previousKeyLength);

        currentPath.remove(currentPath.size() - 1);
    }
}
