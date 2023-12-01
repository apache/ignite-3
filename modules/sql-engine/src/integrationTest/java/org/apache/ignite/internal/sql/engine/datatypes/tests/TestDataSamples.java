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

package org.apache.ignite.internal.sql.engine.datatypes.tests;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Values used in data type tests.
 *
 * @see BaseDataTypeTest
 */
public final class TestDataSamples<T extends Comparable<T>> {

    private final List<T> values;

    private final TreeSet<T> orderedValues;

    private final Map<T, Map<SqlTypeName, Object>> valueReprs;

    private TestDataSamples(List<T> values, Map<T, Map<SqlTypeName, Object>> valueReprs) {
        this.values = values;
        this.orderedValues = new TreeSet<>(values);
        this.valueReprs = valueReprs;

    }

    /**
     * Returns the smallest value among these samples.
     */
    public T min() {
        return orderedValues.first();
    }

    /**
     * Returns the largest value among these samples.
     */
    public T max() {
        return orderedValues.last();
    }

    /**
     * Returns an ordered view of these samples.
     */
    public NavigableSet<T> ordered() {
        return orderedValues;
    }

    /**
     * Returns these samples as a list.
     **/
    public List<T> values() {
        return values;
    }

    /**
     * Returns all representations of the given sample.
     *
     * @throws IllegalArgumentException if there is no such sample.
     */
    public Map<SqlTypeName, Object> get(Object value) {
        return getReprMap(value);
    }

    /**
     * Returns a representation of the given sample of the specified type.
     *
     * @throws IllegalArgumentException if there is no such sample or the there is no representation of this sample of the specified
     *                                  type.
     */
    public Object get(Object value, SqlTypeName typeName) {
        Map<SqlTypeName, Object> map = getReprMap(value);

        Object representation = map.get(typeName);
        if (representation == null) {
            String error = format("No representation of {} type: {}. Types: {}", value, typeName, map.keySet());
            throw new IllegalArgumentException(error);
        }
        return representation;
    }

    private Map<SqlTypeName, Object> getReprMap(Object value) {
        Map<SqlTypeName, Object> map = valueReprs.get((T) value);
        if (map == null) {
            String error = format("No representation of {}. Samples: {}", value, valueReprs.keySet());
            throw new IllegalArgumentException(error);
        }
        return map;
    }

    /**
     * Returns a builder of {@link TestDataSamples}.
     */
    public static <T extends Comparable<T>> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * A builder of {@link TestDataSamples}.
     */
    public static final class Builder<T extends Comparable<T>> {

        private final Map<T, Map<SqlTypeName, Object>> alts = new HashMap<>();

        private final List<T> values = new ArrayList<>();

        private Builder() {

        }

        /**
         * Add a sample with.
         */
        public Builder<T> add(T value, SqlTypeName typeName, Object typeValue) {
            Map<SqlTypeName, Object> vals = alts.computeIfAbsent(value, (key) -> new TreeMap<>());
            if (vals.put(typeName, typeValue) != null) {
                String error = format("Duplicate values can not be present: {} type: {}. Values: {}", value, typeName, values);
                throw new IllegalArgumentException(error);
            }

            if (!values.contains(value)) {
                values.add(value);
            }

            return this;
        }

        /**
         * Adds the given values.
         */
        public Builder<T> add(Collection<T> values, SqlTypeName typeName, Function<T, Object> mapping) {
            for (T value : values) {
                add(value, typeName, mapping.apply(value));
            }

            return this;
        }

        /**
         * Creates an instance of {@link TestDataSamples}.
         **/
        public TestDataSamples<T> build() {
            return new TestDataSamples<>(values, alts);
        }
    }
}
