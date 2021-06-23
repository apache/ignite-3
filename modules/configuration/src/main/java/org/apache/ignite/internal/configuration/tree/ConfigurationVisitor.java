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

package org.apache.ignite.internal.configuration.tree;

import java.io.Serializable;

/**
 * Configuration visitor - callback interface to traverse configuration tree.
 */
public interface ConfigurationVisitor<T> {
    /**
     * Invoked on visiting leaf node.
     *
     * @param key Name of the serializable value retrieved from its holder object.
     * @param val Configuration value.
     * @return Anything that implementation decides to return.
     */
    default T visitLeafNode(String key, Serializable val) {
        return null;
    }

    /**
     * Invoked on visiting regular inner node.
     *
     * @param key Name of the node retrieved from its holder object.
     * @param node Inner configuration node.
     * @return Anything that implementation decides to return.
     */
    default T visitInnerNode(String key, InnerNode node) { //TODO IGNITE-14372 Pass interface, not implementation.
        return null;
    }

    /**
     * Invoked on visiting named list nodes.
     *
     * @param key Name of the node retrieved from its holder object.
     * @param node Named list inner configuration node.
     * @param <N> Type of element nodes in the named list.
     * @return Anything that implementation decides to return.
     */
    default <N extends InnerNode> T visitNamedListNode(String key, NamedListNode<N> node) {
        return null;
    }
}
