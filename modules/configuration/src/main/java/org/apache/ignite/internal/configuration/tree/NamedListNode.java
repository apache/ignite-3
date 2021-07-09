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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.internal.configuration.util.OrderedMap;

/** */
public final class NamedListNode<N extends InnerNode> implements NamedListChange<N>, TraversableTreeNode, ConstructableTreeNode {
    /** Name of synthetic configuration property that describes order of elements in named list. */
    public static final String ORDER_IDX = "<idx>";

    /** */
    public final Supplier<N> valSupplier;

    /** */
    private final OrderedMap<N> map;

    /**
     * Default constructor.
     *
     * @param valSupplier Closure to instantiate values.
     */
    public NamedListNode(Supplier<N> valSupplier) {
        this.valSupplier = valSupplier;
        map = new OrderedMap<>();
    }

    /**
     * Copy constructor.
     *
     * @param node Other node.
     */
    private NamedListNode(NamedListNode<N> node) {
        valSupplier = node.valSupplier;
        map = new OrderedMap<>(node.map);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(String key, ConfigurationVisitor<T> visitor) {
        return visitor.visitNamedListNode(key, this);
    }

    /** {@inheritDoc} */
    @Override public final List<String> namedListKeys() {
        return Collections.unmodifiableList(map.keys());
    }

    /** {@inheritDoc} */
    @Override public final N get(String key) {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N> create(String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valConsumer, "valConsumer");

        if (map.containsKey(key))
            throw new IllegalArgumentException("Element with name " + key + " already exists.");

        N val = valSupplier.get();

        map.put(key, val);

        valConsumer.accept(val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N> create(int index, String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valConsumer, "valConsumer");

        if (index < 0 || index > map.size())
            throw new IndexOutOfBoundsException(index);

        if (map.containsKey(key))
            throw new IllegalArgumentException("Element with name " + key + " already exists.");

        N val = valSupplier.get();

        map.putByIndex(index, key, val);

        valConsumer.accept(val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N> createAfter(String base, String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valConsumer, "valConsumer");

        if (!map.containsKey(base))
            throw new IllegalArgumentException("Element with name " + base + " doesn't exist.");

        if (map.containsKey(key))
            throw new IllegalArgumentException("Element with name " + key + " already exists.");

        N val = valSupplier.get();

        map.putAfter(base, key, val);

        valConsumer.accept(val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public final NamedListChange<N> update(String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valConsumer, "valConsumer");

        if (map.containsKey(key) && map.get(key) == null)
            throw new IllegalArgumentException("You can't create entity that has just been deleted [key=" + key + ']');

        N val = map.get(key);

        if (val == null)
            map.put(key, val = valSupplier.get());
        else
            map.put(key, val = (N)val.copy());

        valConsumer.accept(val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N> delete(String key) {
        Objects.requireNonNull(key, "key");

        if (map.containsKey(key))
            map.put(key, null);

        return this;
    }

    /**
     * Deletes named list element.
     *
     * @param key Element's key.
     */
    public void forceDelete(String key) {
        map.remove(key);
    }

    public void reorderKeys(List<String> orderedKeys) {
        map.reorderKeys(orderedKeys);
    }

    /** {@inheritDoc} */
    @Override public void construct(String key, ConfigurationSource src) {
        if (src == null)
            map.put(key, null);
        else {
            N val = map.get(key);

            val = val == null ? valSupplier.get() : (N)val.copy();

            map.put(key, val);

            src.descend(val);
        }
    }

    /** {@inheritDoc} */
    @Override public NamedListNode<N> copy() {
        return new NamedListNode<>(this);
    }
}
