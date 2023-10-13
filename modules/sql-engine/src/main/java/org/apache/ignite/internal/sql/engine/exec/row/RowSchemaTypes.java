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

package org.apache.ignite.internal.sql.engine.exec.row;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Pair;
import org.jetbrains.annotations.Nullable;

/**
 * Defines methods to provide instances of {@link TypeSpec}s.
 */
public final class RowSchemaTypes {

    /** NULL type. */
    public static final NullTypeSpec NULL = new NullTypeSpec();

    /** Map of {@code <not nullable, nullable>} {@link BaseTypeSpec}s.*/
    static final Map<NativeType, Pair<BaseTypeSpec, BaseTypeSpec>> INTERNED_TYPES;

    static {
        List<NativeType> nativeTypeList = Arrays.asList(
                NativeTypes.BOOLEAN,
                NativeTypes.INT8,
                NativeTypes.INT16,
                NativeTypes.INT32,
                NativeTypes.INT64,
                NativeTypes.FLOAT,
                NativeTypes.DOUBLE,
                NativeTypes.STRING,
                NativeTypes.BYTES,
                NativeTypes.DATE,
                NativeTypes.UUID
        );

        INTERNED_TYPES = nativeTypeList.stream().map(t -> {
            BaseTypeSpec notNull = new BaseTypeSpec(t, false);
            BaseTypeSpec nullable = new BaseTypeSpec(t, true);

            return Map.entry(t, new Pair<>(notNull, nullable));
        }).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private RowSchemaTypes() {

    }

    /** Returns a non-nullable {@link BaseTypeSpec} that corresponds to the given native type without nullability. */
    public static BaseTypeSpec nativeType(NativeType nativeType) {
        return nativeTypeWithNullability(nativeType, false);
    }

    /** Returns a {@link BaseTypeSpec} that corresponds to the given native type with the specified nullability. */
    public static BaseTypeSpec nativeTypeWithNullability(NativeType nativeType, boolean nullable) {
        Pair<BaseTypeSpec, BaseTypeSpec> typePair = INTERNED_TYPES.get(nativeType);

        if (typePair != null) {
            return nullable ? typePair.getSecond() : typePair.getFirst();
        } else {
            return new BaseTypeSpec(nativeType, nullable);
        }
    }

    /**
     * Convert specified schema {@link TypeSpec type} to {@link NativeType native type}.
     *
     * @param type Row schema type.
     * @return Native type or {@code null} if type is a {@link NullTypeSpec}.
     * @throws IllegalArgumentException If provided type cannot be converted to a native type.
     */
    public static @Nullable NativeType toNativeType(TypeSpec type) {
        if (type instanceof NullTypeSpec) {
            return null;
        }

        if (!(type instanceof BaseTypeSpec)) {
            throw new IllegalArgumentException("Unexpected type; " + type);
        }

        NativeType nativeType = ((BaseTypeSpec) type).nativeType();

        assert nativeType != null : type;

        return nativeType;
    }
}
