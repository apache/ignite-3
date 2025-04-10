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

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite3.table.mapper.TypeConverter;
import org.jetbrains.annotations.Nullable;

/** Static Factory for {@link TypeConverter}. Supports some predefined types and Enums. */
public class StaticTypeConverterFactory implements TypeConverterFactory {
    public static final TypeConverterFactory DEFAULT_INSTANCE = new StaticTypeConverterFactory();

    private static final Map<Map.Entry<Class<?>, Class<?>>, TypeConverter<?, ?>> CONVERTERS = new HashMap<>(
            Map.of(
                    Map.entry(Calendar.class, Instant.class), new CalendarToInstantConverter(),
                    Map.entry(Calendar.class, LocalDateTime.class), new CalendarToLocalDateTimeConverter(),
                    Map.entry(Date.class, Instant.class), new DateToInstantConverter(),
                    Map.entry(Date.class, LocalDateTime.class), new DateToLocalDateTimeConverter(),
                    Map.entry(java.sql.Date.class, LocalDate.class), new SqlDateToLocalDateConverter(),
                    Map.entry(Time.class, LocalTime.class), new SqlTimeToLocalTimeConverter(),
                    Map.entry(Timestamp.class, Instant.class), new SqlTimestampToInstantConverter(),
                    Map.entry(Timestamp.class, LocalDateTime.class), new SqlTimestampToLocalDateTimeConverter()
            )
    );

    private StaticTypeConverterFactory() {
        // Intentionally left blank.
    }

    @Override
    public @Nullable <ObjectT, ColumnT> TypeConverter<? extends ObjectT, ? extends ColumnT> converterFor(Class<ObjectT> objType,
            Class<ColumnT> columnType) {
        return (TypeConverter<? extends ObjectT, ? extends ColumnT>)
                CONVERTERS.computeIfAbsent(Map.entry(objType, columnType), StaticTypeConverterFactory::tryCreateNewConverter);
    }

    @Nullable
    private static TypeConverter<?, ?> tryCreateNewConverter(Map.Entry<Class<?>, Class<?>> entry) {
        Class<?> objType = entry.getKey();
        if (objType.isEnum()) {
            return new EnumToStringTypeConverter<>((Class<? extends Enum>) objType);
        } else {
            return null;
        }
    }
}
