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

package org.apache.ignite.internal.configuration.tree;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.configuration.annotation.Secret;

/**
 * {@link ConfigurationVisitor} implementation that converts a configuration tree to a combination of {@link Map} and {@link List} objects.
 */
public class ConverterToMapVisitor implements ConfigurationVisitor<Object> {

    /** Leaf node value will be replaced with this string, if the {@link Field} of the value has {@link Secret} annotation. */
    private static final String MASKED_VALUE = "********";

    /** Include internal configuration nodes (private configuration extensions). */
    private final boolean includeInternal;

    /** Skip nulls, empty Maps and empty lists. */
    private final boolean skipEmptyValues;

    /** Mask values, if their {@link Field} has {@link Secret} annotation. */
    private final boolean maskSecretValues;

    /** Stack with intermediate results. Used to store values during recursive calls. */
    private final Deque<Object> deque = new ArrayDeque<>();

    public static ConverterToMapVisitorBuilder builder() {
        return new ConverterToMapVisitorBuilder();
    }

    /**
     * Constructor.
     *
     * @param includeInternal Include internal configuration nodes (private configuration extensions).
     * @param skipEmptyValues Skip empty values.
     * @param maskSecretValues Mask values, if their {@link Field} has {@link Secret} annotation.
     */
    ConverterToMapVisitor(
            boolean includeInternal,
            boolean skipEmptyValues,
            boolean maskSecretValues
    ) {
        this.includeInternal = includeInternal;
        this.skipEmptyValues = skipEmptyValues;
        this.maskSecretValues = maskSecretValues;
    }

    /** {@inheritDoc} */
    @Override
    public Object visitLeafNode(Field field, String key, Serializable val) {
        Object valObj = val;

        if (val instanceof Character || val instanceof UUID) {
            valObj = val.toString();
        } else if (val != null && val.getClass().isArray()) {
            valObj = toListOfObjects(field, val);
        } else if (val instanceof String) {
            valObj = maskIfNeeded(field, (String) val);
        }

        addToParent(key, valObj);

        return valObj;
    }

    /** {@inheritDoc} */
    @Override
    public Object visitInnerNode(Field field, String key, InnerNode node) {
        if (skipEmptyValues && node == null) {
            return null;
        }

        Map<String, Object> innerMap = new HashMap<>();

        deque.push(innerMap);

        node.traverseChildren(this, includeInternal);

        deque.pop();

        addToParent(key, innerMap);

        return innerMap;
    }

    /** {@inheritDoc} */
    @Override
    public Object visitNamedListNode(Field field, String key, NamedListNode<?> node) {
        if (skipEmptyValues && node.size() == 0) {
            return null;
        }

        List<Object> list = new ArrayList<>(node.size());

        deque.push(list);

        for (String subkey : node.namedListKeys()) {
            node.getInnerNode(subkey).accept(field, subkey, this);

            ((Map<String, Object>) list.get(list.size() - 1)).put(node.syntheticKeyName(), subkey);
        }

        deque.pop();

        addToParent(key, list);

        return list;
    }

    /**
     * Adds a sub-element to the parent object if it exists.
     *
     * @param key Key for the passed element
     * @param val Value to add to the parent
     */
    private void addToParent(String key, Object val) {
        Object parent = deque.peek();

        if (skipEmptyValues && val == null) {
            return;
        }

        if (parent instanceof Map) {
            if (skipEmptyValues && val instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) val;
                if (map.isEmpty()) {
                    return;
                }
            }

            ((Map<String, Object>) parent).put(key, val);
        } else if (parent instanceof List) {
            if (skipEmptyValues && val instanceof List) {
                List<?> list = (List<?>) val;
                if (list.isEmpty()) {
                    return;
                }
            }

            ((Collection<Object>) parent).add(val);
        }
    }

    /**
     * Converts array into a list of objects. Boxes array elements if they are primitive values.
     *
     * @param field Field of the array
     * @param val Array of primitives or array of {@link String}s
     * @return List of objects corresponding to the passed array.
     */
    private List<?> toListOfObjects(Field field, Serializable val) {
        Stream<?> stream = IntStream.range(0, Array.getLength(val)).mapToObj(i -> Array.get(val, i));

        if (val.getClass().getComponentType() == char.class || val.getClass().getComponentType() == UUID.class) {
            stream = stream.map(Object::toString);
        }

        return stream.collect(Collectors.toList());
    }

    /**
     * Returns {@link ConverterToMapVisitor#MASKED_VALUE} if the field is annotated with {@link Secret} and
     * {@link ConverterToMapVisitor#maskSecretValues} is {@code true}.
     *
     * @param field Field to check
     * @param val Value to mask
     * @return Masked value
     */
    private Object maskIfNeeded(Field field, String val) {
        if (!maskSecretValues) {
            return val;
        }

        if (field != null && field.isAnnotationPresent(Secret.class)) {
            return MASKED_VALUE;
        } else {
            return val;
        }
    }
}
