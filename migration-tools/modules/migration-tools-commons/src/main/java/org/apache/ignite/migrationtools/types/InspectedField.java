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

package org.apache.ignite.migrationtools.types;

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/** Holds information about how a field should be persisted. */
public class InspectedField {
    @Nullable
    private final String fieldName;

    private final String typeName;

    private final InspectedFieldType fieldType;

    private final boolean nullable;

    private final boolean hasAnnotation;

    /**
     * Default constructor.
     *
     * @param fieldName Name of the field. May be null for Primitive and Array fields.
     * @param typeName Name of the field class, usually from {@link Class#getName()}
     * @param fieldType Field type.
     * @param nullable Whether the field is nullable or not.
     * @param hasAnnotation Whether the field was annotated with a 'QuerySqlField'.
     */
    private InspectedField(
            @Nullable String fieldName,
            String typeName,
            InspectedFieldType fieldType,
            boolean nullable,
            boolean hasAnnotation
    ) {
        this.fieldName = fieldName;
        this.typeName = typeName;
        this.fieldType = fieldType;
        this.nullable = nullable;
        this.hasAnnotation = hasAnnotation;
    }

    /**
     * Factory method for primitive and array fields.
     *
     * @param typeName Type name.
     * @param fieldType Field type.
     * @return The new InspectedField instance.
     */
    public static InspectedField forUnnamed(String typeName, InspectedFieldType fieldType) {
        if (fieldType != InspectedFieldType.PRIMITIVE && fieldType != InspectedFieldType.ARRAY) {
            throw new IllegalArgumentException("'fieldType' must be PRIMITIVE or ARRAY");
        }

        return new InspectedField(
                null,
                typeName,
                fieldType,
                false,
                false
        );
    }

    /**
     * Factory method for named fields (non-primitive and non-array).
     *
     * @param fieldName Name of the field. May be null for Primitive and Array fields.
     * @param typeName Name of the field class, usually from {@link Class#getName()}
     * @param fieldType Field type.
     * @param nullable Whether the field is nullable or not.
     * @param hasAnnotation Whether the field was annotated with a 'QuerySqlField'.
     * @return The new InspectedField instance.
     */
    public static InspectedField forNamed(
            String fieldName,
            String typeName,
            InspectedFieldType fieldType,
            boolean nullable,
            boolean hasAnnotation
    ) {
        if (fieldType == InspectedFieldType.PRIMITIVE || fieldType == InspectedFieldType.ARRAY) {
            throw new IllegalArgumentException("'fieldType' must not be PRIMITIVE or ARRAY");
        }

        return new InspectedField(
                fieldName,
                typeName,
                fieldType,
                nullable,
                hasAnnotation
        );
    }

    @Nullable
    public String fieldName() {
        return fieldName;
    }

    public String typeName() {
        return typeName;
    }

    public InspectedFieldType fieldType() {
        return fieldType;
    }

    public boolean nullable() {
        return nullable;
    }

    public boolean hasAnnotation() {
        return hasAnnotation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InspectedField that = (InspectedField) o;
        return nullable == that.nullable && hasAnnotation == that.hasAnnotation && Objects.equals(fieldName, that.fieldName)
                && Objects.equals(typeName, that.typeName) && fieldType == that.fieldType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, typeName, fieldType, nullable, hasAnnotation);
    }

    @Override
    public String toString() {
        return "InspectedField{"
                + "fieldName='" + fieldName + '\''
                + ", typeName='" + typeName + '\''
                + ", fieldType=" + fieldType
                + ", nullable=" + nullable
                + ", hasAnnotation=" + hasAnnotation
                + '}';
    }
}
