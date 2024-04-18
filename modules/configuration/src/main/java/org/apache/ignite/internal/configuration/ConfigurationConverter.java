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

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.util.KeyNotFoundException;
import org.apache.ignite.internal.configuration.util.NodeValue;

/**
 * Converter of configuration subtree into a user-defined representation.
 */
public final class ConfigurationConverter {
    private ConfigurationConverter() {
    }

    /**
     * Converts configuration subtree into a user-defined representation.
     *
     * @param superRoot Super root of the configuration.
     * @param path    Path to configuration subtree. Can be empty, can't be {@code null}.
     * @param visitor Visitor that will be applied to the subtree and build the representation.
     * @param <T>     Type of the representation.
     * @return User-defined representation constructed by {@code visitor}.
     * @throws IllegalArgumentException If {@code path} is not found in current configuration.
     */
    public static <T> T convert(SuperRoot superRoot, List<String> path, ConfigurationVisitor<T> visitor) throws IllegalArgumentException {
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
}
