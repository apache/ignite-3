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

import java.util.NoSuchElementException;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;

/**
 * Root configuration.
 */
public class RootInnerNode extends InnerNode {
    /** Root key. */
    private final RootKey<?, ?> key;

    /** Root node. */
    private final InnerNode node;

    /**
     * Constructor.
     *
     * @param node Root node.
     * @param key Root key.
     */
    public RootInnerNode(RootKey<?, ?> key, InnerNode node) {
        this.key = key;
        this.node = node;
    }

    /** {@inheritDoc} */
    @Override public <T> void traverseChildren(ConfigurationVisitor<T> visitor, boolean includeInternal) {
        node.traverseChildren(visitor, includeInternal);
    }

    /** {@inheritDoc} */
    @Override public <T> T traverseChild(
        String key,
        ConfigurationVisitor<T> visitor,
        boolean includeInternal
    ) throws NoSuchElementException {
        return node.traverseChild(key, visitor, includeInternal);
    }

    /** {@inheritDoc} */
    @Override public void construct(String key, ConfigurationSource src, boolean includeInternal) throws NoSuchElementException {
        node.construct(key, src, includeInternal);
    }

    /** {@inheritDoc} */
    @Override public void constructDefault(String fieldName) throws NoSuchElementException {
        node.constructDefault(fieldName);
    }

    /** {@inheritDoc} */
    @Override public Class<?> schemaType() {
        return node.schemaType();
    }

    /** {@inheritDoc} */
    @Override public RootInnerNode copy() {
        return new RootInnerNode(key, node.copy());
    }

    /**
     * Check if the root configuration is marked with {@link InternalConfiguration}.
     *
     * @return {@code true} if the root configuration is internal.
     */
    public boolean internal() {
        return key.internal();
    }
}
