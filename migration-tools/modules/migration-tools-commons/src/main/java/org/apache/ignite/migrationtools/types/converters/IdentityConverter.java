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

/** Identity Converter. */
public class IdentityConverter<T> implements TypeConverter<T, T> {
    public static IdentityConverter INSTANCE = new IdentityConverter();

    @Override
    public T toColumnType(T t) throws Exception {
        return t;
    }

    @Override
    public T toObjectType(T t) throws Exception {
        return t;
    }
}
