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
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Configuration node implementation for the collection of named {@link InnerNode}s. Unlike implementations of
 * {@link InnerNode}, this class is used for every named list in configuration.
 *
 * @param <N> Type of the {@link InnerNode} that is stored in named list node object.
 */
public final class NamedListNode<N extends InnerNode> implements NamedListChange<N>, TraversableTreeNode, ConstructableTreeNode {
    /** Name of a synthetic configuration property that describes the order of elements in a named list. */
    public static final String ORDER_IDX = "<order>";

    /** */
    public static final String ID = "<id>";

    /** Configuration name for the synthetic key. */
    private final String syntheticKeyName;

    /** Supplier of new node objects when new list element node has to be created. */
    private final Supplier<N> valSupplier;

    /** Internal container for named list element. Maps keys to named list elements nodes. */
    private final OrderedMap<IgniteBiTuple<String, N>> map;

    /**
     * Default constructor.
     *
     * @param syntheticKeyName Name of the synthetic configuration value that will represent keys in a specially ordered
     *      representation syntax.
     * @param valSupplier Closure to instantiate values.
     */
    public NamedListNode(String syntheticKeyName, Supplier<N> valSupplier) {
        this.syntheticKeyName = syntheticKeyName;
        this.valSupplier = valSupplier;
        map = new OrderedMap<>();
    }

    /**
     * Copy constructor.
     *
     * @param node Other node.
     */
    private NamedListNode(NamedListNode<N> node) {
        syntheticKeyName = node.syntheticKeyName;
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
        return Optional.ofNullable(map.get(key)).map(IgniteBiTuple::getValue).orElse(null);
    }

    /** {@inheritDoc} */
    @Override public N get(int index) throws IndexOutOfBoundsException {
        return Optional.ofNullable(map.get(index)).map(IgniteBiTuple::getValue).orElse(null);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N> create(String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valConsumer, "valConsumer");

        checkNewKey(key);

        N val = valSupplier.get();

        map.put(key, new IgniteBiTuple<>(newId(), val));

        valConsumer.accept(val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N> create(int index, String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valConsumer, "valConsumer");

        if (index < 0 || index > map.size())
            throw new IndexOutOfBoundsException(index);

        checkNewKey(key);

        N val = valSupplier.get();

        map.putByIndex(index, key, new IgniteBiTuple<>(newId(), val));

        valConsumer.accept(val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N> createAfter(String precedingKey, String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(precedingKey, "precedingKey");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valConsumer, "valConsumer");

        if (!map.containsKey(precedingKey))
            throw new IllegalArgumentException("Element with name " + precedingKey + " doesn't exist.");

        checkNewKey(key);

        N val = valSupplier.get();

        map.putAfter(precedingKey, key, new IgniteBiTuple<>(newId(), val));

        valConsumer.accept(val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public final NamedListChange<N> createOrUpdate(String key, Consumer<N> valConsumer) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valConsumer, "valConsumer");

        if (map.containsKey(key) && map.get(key) == null)
            throw new IllegalArgumentException("You can't create entity that has just been deleted [key=" + key + ']');

        IgniteBiTuple<String, N> pair = map.get(key);

        pair = pair == null
            ? new IgniteBiTuple<>(newId(), valSupplier.get())
            : new IgniteBiTuple<>(pair.getKey(), (N)pair.getValue().copy());

        map.put(key, pair);

        valConsumer.accept(pair.getValue());

        return this;
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N> rename(String oldKey, String newKey) {
        Objects.requireNonNull(oldKey, "oldKey");
        Objects.requireNonNull(newKey, "newKey");

        if (!map.containsKey(oldKey))
            throw new IllegalArgumentException("Element with name " + oldKey + " does not exist.");

        if (map.get(oldKey) == null) {
            throw new IllegalArgumentException(
                "You can't rename entity that has just been deleted [key=" + oldKey + ']'
            );
        }

        checkNewKey(newKey);

        map.rename(oldKey, newKey);

        return this;
    }

    private static String newId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * Checks that this new key can be inserted into the map.
     * @param key New key.
     * @throws IllegalArgumentException If key already exists.
     */
    private void checkNewKey(String key) {
        if (map.containsKey(key)) {
            if (map.get(key) == null)
                throw new IllegalArgumentException("You can't create entity that has just been deleted [key=" + key + ']');

            throw new IllegalArgumentException("Element with name " + key + " already exists.");
        }
    }

    /** {@inheritDoc} */
    @Override public NamedListChange<N> delete(String key) {
        Objects.requireNonNull(key, "key");

        if (map.containsKey(key))
            map.put(key, null);

        return this;
    }

    /**
     * @return Configuration name for the synthetic key.
     *
     * @see NamedConfigValue#syntheticKeyName()
     */
    public String syntheticKeyName() {
        return syntheticKeyName;
    }

    public void setInternalId(String key, String id) {
        IgniteBiTuple<String, N> pair = map.get(key);

        if (pair != null)
            pair.set1(id);
    }

    public String internalId(String key) {
        return Optional.ofNullable(map.get(key)).map(IgniteBiTuple::getKey).orElseThrow(
            () -> new IllegalArgumentException("Element with name " + key + " does not exist.")
        );
    }

    /**
     * Deletes named list element.
     *
     * @param key Element's key.
     */
    public void forceDelete(String key) {
        map.remove(key);
    }

    /**
     * Reorders keys in the map.
     *
     * @param orderedKeys List of keys in new order. Must have the same set of keys in it.
     */
    public void reorderKeys(List<String> orderedKeys) {
        map.reorderKeys(orderedKeys);
    }

    /** {@inheritDoc} */
    @Override public void construct(String key, ConfigurationSource src) {
        if (src == null)
            delete(key);
        else
            createOrUpdate(key, src::descend);
    }

    /** {@inheritDoc} */
    @Override public NamedListNode<N> copy() {
        return new NamedListNode<>(this);
    }
}
