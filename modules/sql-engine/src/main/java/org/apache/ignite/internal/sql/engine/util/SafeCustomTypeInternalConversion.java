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

import java.util.EnumMap;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomTypeSpec;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * The sole purpose of this class is to provide safe-conversion to and from internal types,
 * preventing class cast exceptions inside when using {@link TypeUtils} methods for to/from type conversion.
 */
final class SafeCustomTypeInternalConversion {

    static final SafeCustomTypeInternalConversion INSTANCE = new SafeCustomTypeInternalConversion(Commons.typeFactory());

    private final EnumMap<ColumnType, Class<?>> internalTypes = new EnumMap<>(ColumnType.class);

    private SafeCustomTypeInternalConversion(IgniteTypeFactory typeFactory) {
        // IgniteCustomType: We can automatically compute val -> internal type mapping
        // by using type specs from the type factory.
        var customTypes = typeFactory.getCustomTypeSpecs();

        for (IgniteCustomTypeSpec t : customTypes.values()) {
            internalTypes.put(t.nativeType().spec(), t.storageType());
        }
    }

    @Nullable
    Object tryConvertToInternal(Object val, ColumnType storageType) {
        Class<?> internalType = internalTypes.get(storageType);

        assert internalType == null || internalType.isInstance(val) : storageTypeMismatch(val, internalType);

        return val;
    }

    Object tryConvertFromInternal(Object val, ColumnType storageType) {
        Class<?> internalType = internalTypes.get(storageType);

        assert internalType == null || internalType.isInstance(val) : storageTypeMismatch(val, internalType);

        return val;
    }

    private static String storageTypeMismatch(Object value, Class<?> type) {
        return String.format("storageType is %s value must also be %s but it was: %s", type, type, value);
    }
}
