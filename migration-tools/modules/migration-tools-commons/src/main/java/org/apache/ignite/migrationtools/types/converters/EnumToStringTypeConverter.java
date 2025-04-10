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

package org.apache.ignite.migrationtools.types.converters;

import org.apache.ignite3.table.mapper.TypeConverter;

/** Converts an Enum to String. */
public class EnumToStringTypeConverter<T extends Enum<T>> implements TypeConverter<Enum<T>, String> {
    private final Class<T> enumType;

    public EnumToStringTypeConverter(Class<T> type) {
        enumType = type;
    }

    @Override
    public String toColumnType(Enum<T> anEnum) throws Exception {
        return anEnum.name();
    }

    @Override
    public Enum<T> toObjectType(String name) throws Exception {
        return Enum.valueOf(enumType, name);
    }
}
