/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.types.converters;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import org.apache.ignite3.table.mapper.TypeConverter;

/** Converts SQL Timestamp to LocalDateTime. */
public class SqlTimestampToLocalDateTimeConverter implements TypeConverter<Timestamp, LocalDateTime> {
    @Override
    public LocalDateTime toColumnType(Timestamp timestamp) throws Exception {
        return timestamp.toLocalDateTime();
    }

    @Override
    public Timestamp toObjectType(LocalDateTime time) throws Exception {
        return Timestamp.valueOf(time);
    }
}
