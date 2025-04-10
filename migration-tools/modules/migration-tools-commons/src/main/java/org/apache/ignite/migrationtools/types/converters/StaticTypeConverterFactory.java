/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
