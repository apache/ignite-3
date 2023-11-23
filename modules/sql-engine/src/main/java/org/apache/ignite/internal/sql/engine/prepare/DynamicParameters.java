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

package org.apache.ignite.internal.sql.engine.prepare;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.NullableValue;
import org.jetbrains.annotations.Nullable;

/** A container for dynamic parameters. */
public final class DynamicParameters {

    private static final DynamicParameters EMPTY = builder().build();

    private final List<NullableValue<Object>> values;

    private DynamicParameters(List<NullableValue<Object>> values) {
        this.values = values;
    }

    /**
     * Returns a {@link NullableValue nullable value} that contains a value of {@code index}-th (zero based) parameter,
     * or {@code null} if that parameter is not set .
     *
     * @param index Parameter index.
     * @return Nullable parameter value or {@code null} if that parameter is not specified.
     */
    @Nullable
    public NullableValue<Object> value(int index) {
        return values.get(index);
    }

    /** Returns the number of parameters. */
    public int size() {
        return values.size();
    }

    /** Returns {@code true} if the number of parameter is {@code 0}. Otherwise returns {@code false}. */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Creates an array of parameters values. Example:
     * <pre>
     *     Parameters: ["a", 4, null]
     *     Array: ["a", 4, null]
     * </pre>
     *
     * <p>If some parameters are not specified, throws {@link IllegalStateException}.
     *
     * @return Array of parameter values.
     * @throws IllegalStateException if some parameters are not specified.
     */
    public Object[] toArray() {
        Object[] result = new Object[values.size()];

        for (int i = 0; i < values.size(); i++) {
            Object object = getParamValue(i);
            result[i] = object;
        }

        return result;
    }

    /**
     * Creates a map of parameters values. Example:
     * <pre>
     *     Parameters: ["a", 4, null]
     *     Map: {"?0": "a", "?1": 4, "?2": null}
     * </pre>
     *
     * <p>If some parameters are not specified, throws {@link IllegalStateException}.
     *
     * @return Map of parameter values.
     * @throws IllegalStateException if some parameters are not specified.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> result = new HashMap<>();

        for (int i = 0; i < values.size(); i++) {
            Object value = getParamValue(i);
            result.put("?" + i, value);
        }

        return Collections.unmodifiableMap(result);
    }

    @Nullable
    private Object getParamValue(int i) {
        NullableValue<Object> paramValue = values.get(i);
        if (paramValue == null) {
            String message = format("Not all parameters have been specified: {}" + this);
            throw new IllegalStateException(message);
        }
        return paramValue.get();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");

        for (int i = 0; i < values.size(); i++) {
            NullableValue<Object> value = values.get(i);
            if (i > 0) {
                sb.append(", ");
            }

            sb.append(i);
            sb.append(": ");
            if (value == null) {
                sb.append("<none>");
            } else {
                sb.append(value.get());
            }
        }
        sb.append("}");

        return sb.toString();
    }

    /** Returns empty dynamic parameters. */
    public static DynamicParameters empty() {
        return EMPTY;
    }

    /**
     * Creates dynamic parameter container for the given varargs.
     *
     * @param values Array of values.
     * @return Dynamic parameters container.
     */
    public static DynamicParameters of(Object... values) {
        if (values.length == 0) {
            return empty();
        }

        DynamicParameters.Builder builder = new Builder();
        for (int i = 0; i < values.length; i++) {
            builder.set(i, values[i]);
        }
        return builder.build();
    }

    /** Creates a builder for dynamic parameters. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for dynamic parameters. */
    public static class Builder {

        private final List<Pair<Integer, Object>> values = new ArrayList<>();

        Builder() {

        }

        /**
         * Sets {@code index}-th parameter.
         *
         * @param index Parameter index.
         * @param value Parameter value.
         * @return this for chaining.
         */
        public Builder set(int index, @Nullable Object value) {
            values.add(new Pair<>(index, value));
            return this;
        }

        /** Creates a dynamic parameters container. */
        public DynamicParameters build() {
            List<NullableValue<Object>> out = new ArrayList<>();
            int minIndex = 0;

            for (Pair<Integer, Object> idxValue : values) {
                int paramNum = idxValue.getFirst();
                Object paramValue = idxValue.getSecond();

                for (int j = minIndex; j < paramNum; j++) {
                    out.add(null);
                }

                out.add(NullableValue.of(paramValue));
                minIndex = paramNum + 1;
            }

            return new DynamicParameters(out);
        }
    }
}
