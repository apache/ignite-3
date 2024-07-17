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

package org.apache.ignite.internal.sql.engine.type;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;

/**
 * Specification that defines properties of a {@link IgniteCustomType custom data type} that are shared by all instances of that type.
 *
 * @see IgniteCustomType custom data type.
 */
public final class IgniteCustomTypeSpec {

    private final String typeName;

    private final NativeType nativeType;

    private final ColumnType columnType;

    private final Class<?> storageType;

    private final Method castFunction;

    /**
     * Creates a specification for a type that with the specified {@code storage type}.
     *
     * @param typeName Type name.
     * @param nativeType Native type.
     * @param columnType Column type.
     * @param storageType Storage type.
     * @param castFunction Cast function.
     */
    public IgniteCustomTypeSpec(String typeName, NativeType nativeType, ColumnType columnType,
            Class<? extends Comparable<?>> storageType, Method castFunction) {

        this.typeName = typeName;
        this.nativeType = nativeType;
        this.columnType = columnType;
        this.storageType = storageType;
        this.castFunction = castFunction;
    }

    /** Returns the name of the type. */
    public String typeName() {
        return typeName;
    }

    /**
     * Returns the {@link NativeType} for this custom data type.
     *
     * <p>At the moment it serves the following purpose:
     * <ul>
     *     <li>
     *         Used by {@link IgniteTypeFactory#relDataTypeToNative(RelDataType)} to retrieve underlying
     *        {@link NativeType} for DDL queries.
     *     </li>
     *     <li>
     *         To retrieve a java type to perform type conversions by
     *         {@link ExecutionServiceImpl}.
     *     </li>
     * </ul>
     */
    public NativeType nativeType() {
        return nativeType;
    }

    /**
     * Returns the {@link ColumnType} of this data type. Provides type information for {@link ColumnMetadata}.
     */
    public ColumnType columnType() {
        return columnType;
    }

    /**
     * Returns the storage type of this data type.
     *
     * <p>This method is called by {@link IgniteTypeFactory#getJavaClass(RelDataType)}
     * to provide types for a expression interpreter. Execution engine also relies on the fact that this type is also used by
     * {@link TypeUtils TypeUtils} in type conversions.
     *
     * @see ExpressionFactoryImpl
     * @see TypeUtils#toInternal(Object, Type)
     * @see TypeUtils#fromInternal(Object, Type)
     */
    public Class<?> storageType() {
        return storageType;
    }

    /** Implementation of a cast function for this type. */
    public Method castFunction() {
        return castFunction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IgniteCustomTypeSpec that = (IgniteCustomTypeSpec) o;
        return typeName.equals(that.typeName) && nativeType.equals(that.nativeType) && storageType.equals(that.storageType)
                && columnType == that.columnType;
    }

    /**
     * Returns a method that implements a cast function for a custom data type.
     *
     * <p>Requirements:
     * <ul>
     *     <li>It must be a {@code public static} method.</li>
     *     <li>It must have the following signature: {@code Object -> typeSpec::nativeType.javaClass}.</li>
     *     <li>It must implement conversion from the type itself.</li>
     *     <li>It must implement conversion from String to {@code typeSpec::nativeType.javaClass}.</li>
     * </ul>
     *
     * @param clazz A class that defines a cast function.
     * @param method A name of a method.
     * @return A method that implements a cast function.
     */
    public static Method getCastFunction(Class<?> clazz, String method) {
        try {
            return clazz.getMethod(method, Object.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Incorrect cast function method: " + method, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(typeName, nativeType, storageType, columnType);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(IgniteCustomTypeSpec.class, this);
    }
}
