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

package org.apache.ignite.internal.configuration.compatibility.framework;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

/**
 * Annotation property value.
 */
public class ConfigAnnotationValue {
    @JsonProperty
    private String typeName;
    @JsonProperty
    private Object value;

    @SuppressWarnings("unused")
    public ConfigAnnotationValue() {
        // Default constructor for Jackson deserialization.
    }

    private ConfigAnnotationValue(String typeName, Object value) {
        this.typeName = typeName;
        this.value = value;
    }

    /**
     * Creates a value for a non-array property.
     *
     * @param className Class name.
     * @param value Value.
     * @return Value.
     */
    public static ConfigAnnotationValue createValue(String className, Object value) {
        return new ConfigAnnotationValue(className, value);
    }

    /**
     * Creates a value for an array property.
     *
     * @param elementClassName Element type name.
     * @param elements Elements.
     * @return Value.
     */
    public static ConfigAnnotationValue createArray(String elementClassName, List<Object> elements) {
        return new ConfigAnnotationValue(elementClassName + "[]", elements);
    }

    /**
     * Returns a value.
     */
    public Object value() {
        return value;
    }

    /**
     * Returns a type name.
     */
    public String typeName() {
        return typeName;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConfigAnnotationValue that = (ConfigAnnotationValue) o;
        return Objects.equals(typeName, that.typeName) && Objects.equals(value, that.value);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(typeName, value);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return typeName + ":" + value;
    }
}
