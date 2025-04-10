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
import org.jetbrains.annotations.Nullable;

/** Factory for {@link org.apache.ignite3.table.mapper.TypeConverter} based on the required types. */
public interface TypeConverterFactory {
    @Nullable
    <ObjectT, ColumnT> TypeConverter<? extends ObjectT, ? extends ColumnT> converterFor(Class<ObjectT> objType, Class<ColumnT> columnType);
}
