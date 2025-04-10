/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
