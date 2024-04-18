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

package org.apache.ignite.internal.sql.engine.util;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.jetbrains.annotations.Nullable;

/**
 * The sole purpose of this class is to provide safe-conversion to and from internal types,
 * preventing class cast exceptions inside when using {@link TypeUtils} methods for to/from type conversion.
 */
final class SafeCustomTypeInternalConversion {

    static final SafeCustomTypeInternalConversion INSTANCE = new SafeCustomTypeInternalConversion(Commons.typeFactory());

    private final Map<NativeTypeSpec, Class<?>> internalTypes;

    private SafeCustomTypeInternalConversion(IgniteTypeFactory typeFactory) {
        // IgniteCustomType: We can automatically compute val -> internal type mapping
        // by using type specs from the type factory.
        var customTypes = typeFactory.getCustomTypeSpecs();

        internalTypes = customTypes.values()
                .stream()
                .map(t -> Map.entry(t.nativeType().spec(), t.storageType()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    @Nullable
    Object tryConvertToInternal(Object val, NativeTypeSpec storageType) {
        Class<?> internalType = internalTypes.get(storageType);
        if (internalType == null) {
            return null;
        }

        assert internalType.isInstance(val) : storageTypeMismatch(val, internalType);
        return val;
    }

    @Nullable
    Object tryConvertFromInternal(Object val, NativeTypeSpec storageType) {
        Class<?> internalType = internalTypes.get(storageType);
        if (internalType == null) {
            return null;
        }

        assert internalType.isInstance(val) : storageTypeMismatch(val, internalType);
        return val;
    }

    private static String storageTypeMismatch(Object value, Class<?> type) {
        return String.format("storageType is %s value must also be %s but it was: %s", type, type, value);
    }
}
